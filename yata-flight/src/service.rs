use std::pin::Pin;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow_array::StringArray;
use lancedb::query::{ExecutableQuery, QueryBase};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
    decode::FlightRecordBatchStream,
    encode::FlightDataEncoderBuilder,
    error::{FlightError, Result as FlightResult},
    flight_service_server::FlightService,
};
use arrow_schema::{DataType, Field, Schema};
use futures::{Stream, TryStreamExt};
use indexmap::IndexMap;
use tonic::{Request, Response, Status, Streaming};

use crate::catalog::YataTableCatalog;
use crate::codec::{
    AnyPutTicket, AnyTicket, CypherMutateResult, CypherMutateTicket, CypherTicket, ScanTicket,
    VectorSearchTicket,
};

pub struct YataFlightService {
    catalog: Arc<YataTableCatalog>,
    conn: lancedb::Connection,
    /// Base URI for graph_vertices / graph_edges Lance tables.
    /// When set, enables Cypher queries via `CypherTicket`.
    graph_base_uri: Option<String>,
}

impl YataFlightService {
    pub async fn new(lance_base_uri: impl Into<String>) -> anyhow::Result<Self> {
        let base_uri = lance_base_uri.into();
        let conn = lancedb::connect(&base_uri)
            .execute()
            .await
            .map_err(|e| anyhow::anyhow!("lancedb connect: {e}"))?;
        Ok(Self {
            catalog: Arc::new(YataTableCatalog::new(base_uri)),
            conn,
            graph_base_uri: None,
        })
    }

    /// Create service with graph store support for Cypher queries.
    pub async fn new_with_graph(
        lance_base_uri: impl Into<String>,
        graph_base_uri: impl Into<String>,
    ) -> anyhow::Result<Self> {
        let base_uri = lance_base_uri.into();
        let conn = lancedb::connect(&base_uri)
            .execute()
            .await
            .map_err(|e| anyhow::anyhow!("lancedb connect: {e}"))?;
        Ok(Self {
            catalog: Arc::new(YataTableCatalog::new(base_uri)),
            conn,
            graph_base_uri: Some(graph_base_uri.into()),
        })
    }

    /// Serialize an Arrow schema to IPC message bytes (schema message + EOS marker).
    /// StreamWriter writes the schema IPC message in `try_new`, and finish() appends
    /// the EOS marker. Flight clients parse only the schema message and ignore the EOS.
    fn schema_ipc_bytes(schema: &Arc<Schema>) -> bytes::Bytes {
        let mut buf = Vec::new();
        if let Ok(mut writer) =
            arrow::ipc::writer::StreamWriter::try_new(&mut buf, schema.as_ref())
        {
            let _ = writer.finish();
        }
        bytes::Bytes::from(buf)
    }

    /// Parse a FlightDescriptor into a ScanTicket.
    /// CMD → JSON-encoded ScanTicket bytes; PATH → path[0] is the table name.
    fn parse_descriptor(desc: &FlightDescriptor) -> Result<ScanTicket, Status> {
        if !desc.cmd.is_empty() {
            ScanTicket::from_bytes(&desc.cmd)
                .map_err(|e| Status::invalid_argument(format!("invalid cmd: {e}")))
        } else if !desc.path.is_empty() {
            Ok(ScanTicket::table(desc.path[0].clone()))
        } else {
            Err(Status::invalid_argument("empty descriptor"))
        }
    }

    fn build_flight_info(
        catalog: &YataTableCatalog,
        table: &str,
        desc: FlightDescriptor,
        ticket: ScanTicket,
    ) -> Result<FlightInfo, Status> {
        let schema = catalog
            .schema(table)
            .ok_or_else(|| Status::not_found(format!("table not found: {table}")))?;
        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(e.to_string()))?
            .with_descriptor(desc)
            .with_endpoint(FlightEndpoint::new().with_ticket(Ticket::new(ticket.to_bytes())))
            .with_total_records(-1)
            .with_total_bytes(-1);
        Ok(info)
    }

    async fn execute_scan(
        catalog: Arc<YataTableCatalog>,
        conn: lancedb::Connection,
        ticket: ScanTicket,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>, Status>
    {
        let schema = catalog
            .schema(&ticket.table)
            .ok_or_else(|| Status::not_found(format!("table not found: {}", ticket.table)))?;

        let tbl = match conn.open_table(&ticket.table).execute().await {
            Ok(t) => t,
            Err(e) => {
                tracing::debug!(table = %ticket.table, err = %e, "dataset not found, returning empty");
                // Return schema-only stream (no rows)
                let empty = futures::stream::empty::<FlightResult<RecordBatch>>();
                let stream = FlightDataEncoderBuilder::new()
                    .with_schema(schema)
                    .build(empty)
                    .map_err(tonic::Status::from);
                return Ok(Box::pin(stream));
            }
        };

        let mut q = tbl.query();

        if let Some(filter) = &ticket.filter {
            q = q.only_if(filter.as_str());
        }
        if let Some(limit) = ticket.limit {
            q = q.limit(limit as usize);
        }
        // Note: projection and offset are not supported in this lancedb query path.

        let batch_stream = q
            .execute()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let flight_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream.map_err(|e| FlightError::ExternalError(Box::new(e))))
            .map_err(tonic::Status::from);

        Ok(Box::pin(flight_stream))
    }

    /// Execute a Cypher query against the tiered graph store (LanceGraph → Lance → B2).
    ///
    /// Steps:
    ///   1. Load graph_vertices + graph_edges from Lance into MemoryGraph.
    ///   2. Execute Cypher with yata-cypher executor.
    ///   3. Infer Arrow schema from result column names (all Utf8 — JSON-encoded values).
    ///   4. Return as Arrow IPC stream via FlightDataEncoderBuilder.
    ///
    /// Shannon efficiency: column names transmitted once in schema, not per row.
    /// For N rows with M columns of avg name length L: saves (N-1)×M×L bytes vs JSON rows.
    async fn execute_cypher_scan(
        ticket: CypherTicket,
        default_graph_base_uri: Option<&str>,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>, Status>
    {
        let graph_uri = ticket
            .graph_uri
            .as_deref()
            .or(default_graph_base_uri)
            .ok_or_else(|| Status::failed_precondition("no graph_base_uri configured"))?;

        let store = yata_graph::LanceGraphStore::new(graph_uri)
            .await
            .map_err(|e| Status::internal(format!("graph store: {e}")))?;

        let mut graph = store
            .to_memory_graph()
            .await
            .map_err(|e| Status::internal(format!("graph load: {e}")))?;

        let raw_rows = graph
            .query(&ticket.cypher, &ticket.params)
            .map_err(|e| Status::internal(format!("cypher: {e}")))?;

        if raw_rows.is_empty() {
            // Empty result — return empty stream with zero-column schema.
            let schema = Arc::new(Schema::empty());
            let empty = futures::stream::empty::<FlightResult<RecordBatch>>();
            let stream = FlightDataEncoderBuilder::new()
                .with_schema(schema)
                .build(empty)
                .map_err(tonic::Status::from);
            return Ok(Box::pin(stream));
        }

        // Collect column names from the first row (defines the schema).
        // All columns are Utf8 — values are JSON-encoded Cypher values.
        let col_names: Vec<String> = raw_rows[0].iter().map(|(name, _)| name.clone()).collect();
        let fields: Vec<Field> = col_names
            .iter()
            .map(|name| Field::new(name.as_str(), DataType::Utf8, true))
            .collect();
        let schema = Arc::new(Schema::new(fields));

        // Build columnar arrays (one StringArray per column).
        let n_rows = raw_rows.len();
        let mut columns: Vec<Vec<Option<String>>> = vec![vec![None; n_rows]; col_names.len()];

        for (row_idx, row) in raw_rows.iter().enumerate() {
            for (col_name, json_val) in row {
                if let Some(col_idx) = col_names.iter().position(|c| c == col_name) {
                    columns[col_idx][row_idx] = Some(json_val.clone());
                }
            }
        }

        let arrays: Vec<Arc<dyn arrow_array::Array>> = columns
            .into_iter()
            .map(|col| {
                let arr: StringArray = col.into_iter().collect();
                Arc::new(arr) as Arc<dyn arrow_array::Array>
            })
            .collect();

        let batch = RecordBatch::try_new(schema.clone(), arrays)
            .map_err(|e| Status::internal(format!("arrow batch: {e}")))?;

        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::once(async move { Ok(batch) }))
            .map_err(tonic::Status::from);

        Ok(Box::pin(stream))
    }

    /// Execute a vector nearest-neighbor search on a Lance table.
    async fn execute_vector_search(
        conn: lancedb::Connection,
        ticket: VectorSearchTicket,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>, Status>
    {
        let tbl = conn
            .open_table(&ticket.table)
            .execute()
            .await
            .map_err(|e| Status::not_found(format!("table: {e}")))?;

        let mut q = tbl
            .vector_search(ticket.vector)
            .map_err(|e| Status::internal(format!("vector_search: {e}")))?
            .column(&ticket.column)
            .limit(ticket.limit);

        if let Some(np) = ticket.nprobes {
            q = q.nprobes(np);
        }
        if let Some(ref f) = ticket.filter {
            q = q.only_if(f.as_str());
        }

        let batch_stream = q
            .execute()
            .await
            .map_err(|e| Status::internal(format!("vector exec: {e}")))?;

        let first_batch = batch_stream
            .try_collect::<Vec<_>>()
            .await
            .map_err(|e| Status::internal(format!("vector stream: {e}")))?;

        let schema = if let Some(b) = first_batch.first() {
            b.schema()
        } else {
            Arc::new(Schema::empty())
        };

        let flight_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::iter(
                first_batch.into_iter().map(|b| Ok::<_, FlightError>(b)),
            ))
            .map_err(tonic::Status::from);

        Ok(Box::pin(flight_stream))
    }

    // ---- Write operations -------------------------------------------------

    /// Collect all RecordBatches from a FlightData stream (after descriptor extraction).
    async fn collect_batches(
        data: Vec<FlightData>,
    ) -> Result<Vec<RecordBatch>, Status> {
        if data.is_empty() {
            return Ok(Vec::new());
        }
        let stream = FlightRecordBatchStream::new_from_flight_data(
            futures::stream::iter(data.into_iter().map(Ok::<_, FlightError>)),
        );
        stream
            .try_collect()
            .await
            .map_err(|e| Status::internal(format!("decode flight data: {e}")))
    }

    /// Concatenate multiple RecordBatches into one (same schema required).
    fn concat_batches(batches: &[RecordBatch]) -> Result<RecordBatch, Status> {
        if batches.is_empty() {
            return Err(Status::invalid_argument("no record batches in put stream"));
        }
        arrow::compute::concat_batches(&batches[0].schema(), batches)
            .map_err(|e| Status::internal(format!("concat batches: {e}")))
    }

    /// Execute Lance table append: collect all batches → single append.
    async fn execute_lance_write(
        conn: lancedb::Connection,
        table: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<usize, Status> {
        if batches.is_empty() {
            return Ok(0);
        }
        let merged = Self::concat_batches(&batches)?;
        let row_count = merged.num_rows();
        let schema = merged.schema();

        let reader = arrow_array::RecordBatchIterator::new(
            std::iter::once(Ok::<_, arrow::error::ArrowError>(merged)),
            schema,
        );

        match conn.open_table(table).execute().await {
            Ok(tbl) => {
                tbl.add(reader)
                    .execute()
                    .await
                    .map_err(|e| Status::internal(format!("lance append: {e}")))?;
            }
            Err(_) => {
                conn.create_table(table, reader)
                    .execute()
                    .await
                    .map_err(|e| Status::internal(format!("lance create: {e}")))?;
            }
        }

        Ok(row_count)
    }

    /// Execute graph vertex/edge batch write via LanceGraphStore.
    async fn execute_graph_write(
        graph_uri: &str,
        target: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<usize, Status> {
        if batches.is_empty() {
            return Ok(0);
        }
        let merged = Self::concat_batches(&batches)?;
        let row_count = merged.num_rows();

        let store = yata_graph::LanceGraphStore::new(graph_uri)
            .await
            .map_err(|e| Status::internal(format!("graph store: {e}")))?;

        match target {
            "vertices" => {
                store
                    .write_vertices_batch(merged)
                    .await
                    .map_err(|e| Status::internal(format!("write vertices: {e}")))?;
            }
            "edges" => {
                store
                    .write_edges_batch(merged)
                    .await
                    .map_err(|e| Status::internal(format!("write edges: {e}")))?;
            }
            other => return Err(Status::invalid_argument(format!("unknown target: {other}"))),
        }

        Ok(row_count)
    }

    /// Execute a Cypher mutation: load graph → exec → diff → batch write delta.
    async fn execute_cypher_mutate(
        ticket: CypherMutateTicket,
        default_graph_base_uri: Option<&str>,
    ) -> Result<CypherMutateResult, Status> {
        use yata_cypher::Graph;

        let graph_uri = ticket
            .graph_uri
            .as_deref()
            .or(default_graph_base_uri)
            .ok_or_else(|| Status::failed_precondition("no graph_base_uri configured"))?;

        let store = yata_graph::LanceGraphStore::new(graph_uri)
            .await
            .map_err(|e| Status::internal(format!("graph store: {e}")))?;

        let mut graph = store
            .to_memory_graph()
            .await
            .map_err(|e| Status::internal(format!("graph load: {e}")))?;

        // Snapshot before mutation
        let before_nodes: IndexMap<String, yata_cypher::NodeRef> = graph
            .0
            .nodes()
            .into_iter()
            .map(|n| (n.id.clone(), n))
            .collect();
        let before_edges: IndexMap<String, yata_cypher::RelRef> = graph
            .0
            .rels()
            .into_iter()
            .map(|r| (r.id.clone(), r))
            .collect();

        // Parse + execute Cypher mutation
        let query = yata_cypher::parse(&ticket.cypher)
            .map_err(|e| Status::invalid_argument(format!("cypher parse: {e}")))?;

        let mut param_map = IndexMap::new();
        for (k, v) in &ticket.params {
            let val: serde_json::Value =
                serde_json::from_str(v).unwrap_or(serde_json::Value::String(v.clone()));
            param_map.insert(k.clone(), yata_graph::json_to_cypher(&val));
        }

        let _result = yata_cypher::Executor::with_params(param_map)
            .execute(&query, &mut graph.0)
            .map_err(|e| Status::internal(format!("cypher exec: {e}")))?;

        // Diff + batch write delta
        let stats = store
            .write_delta(&before_nodes, &before_edges, &graph.0)
            .await
            .map_err(|e| Status::internal(format!("write delta: {e}")))?;

        Ok(CypherMutateResult {
            nodes_created: stats.nodes_created,
            nodes_modified: stats.nodes_modified,
            nodes_deleted: stats.nodes_deleted,
            edges_created: stats.edges_created,
            edges_modified: stats.edges_modified,
            edges_deleted: stats.edges_deleted,
        })
    }
}

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl FlightService for YataFlightService {
    type HandshakeStream = BoxStream<HandshakeResponse>;
    type ListFlightsStream = BoxStream<FlightInfo>;
    type DoGetStream = BoxStream<FlightData>;
    type DoPutStream = BoxStream<PutResult>;
    type DoExchangeStream = BoxStream<FlightData>;
    type DoActionStream = BoxStream<arrow_flight::Result>;
    type ListActionsStream = BoxStream<ActionType>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake not supported"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        let catalog = &self.catalog;
        let infos: Vec<Result<FlightInfo, Status>> = catalog
            .tables()
            .iter()
            .filter_map(|&table| {
                let schema = catalog.schema(table)?;
                let ticket = ScanTicket::table(table);
                let info = FlightInfo::new()
                    .try_with_schema(&schema)
                    .ok()?
                    .with_descriptor(FlightDescriptor::new_path(vec![table.to_string()]))
                    .with_endpoint(
                        FlightEndpoint::new().with_ticket(Ticket::new(ticket.to_bytes())),
                    )
                    .with_total_records(-1)
                    .with_total_bytes(-1);
                Some(Ok(info))
            })
            .collect();

        Ok(Response::new(Box::pin(futures::stream::iter(infos))))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let desc = request.into_inner();
        let ticket = Self::parse_descriptor(&desc)?;
        let info = Self::build_flight_info(&self.catalog, &ticket.table.clone(), desc, ticket)?;
        Ok(Response::new(info))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info not supported"))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let desc = request.into_inner();
        let ticket = Self::parse_descriptor(&desc)?;
        let schema = self
            .catalog
            .schema(&ticket.table)
            .ok_or_else(|| Status::not_found(format!("table not found: {}", ticket.table)))?;
        Ok(Response::new(SchemaResult {
            schema: Self::schema_ipc_bytes(&schema),
        }))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let raw_ticket = request.into_inner();
        match AnyTicket::from_bytes(&raw_ticket.ticket)
            .map_err(|e| Status::invalid_argument(e.to_string()))?
        {
            AnyTicket::Scan(ticket) => {
                tracing::debug!(
                    table = %ticket.table,
                    filter = ?ticket.filter,
                    limit = ?ticket.limit,
                    "do_get scan"
                );
                let stream = Self::execute_scan(self.catalog.clone(), self.conn.clone(), ticket).await?;
                Ok(Response::new(stream))
            }
            AnyTicket::Cypher(ticket) => {
                tracing::debug!(
                    cypher = %ticket.cypher,
                    params = ?ticket.params,
                    "do_get cypher"
                );
                let stream =
                    Self::execute_cypher_scan(ticket, self.graph_base_uri.as_deref()).await?;
                Ok(Response::new(stream))
            }
            AnyTicket::VectorSearch(ticket) => {
                tracing::debug!(
                    table = %ticket.table,
                    column = %ticket.column,
                    limit = ticket.limit,
                    "do_get vector_search"
                );
                let stream =
                    Self::execute_vector_search(self.conn.clone(), ticket).await?;
                Ok(Response::new(stream))
            }
        }
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let mut stream = request.into_inner();

        // Collect all FlightData; extract descriptor from first message.
        let mut all_data: Vec<FlightData> = Vec::new();
        let mut descriptor: Option<FlightDescriptor> = None;

        while let Some(data) = stream.message().await? {
            if descriptor.is_none() {
                descriptor = data.flight_descriptor.clone();
            }
            all_data.push(data);
        }

        let desc = descriptor
            .ok_or_else(|| Status::invalid_argument("empty do_put stream"))?;

        let put_ticket = AnyPutTicket::from_bytes(&desc.cmd)
            .map_err(|e| Status::invalid_argument(format!("invalid put ticket: {e}")))?;

        // Decode FlightData → RecordBatches
        let batches = Self::collect_batches(all_data).await?;

        let row_count = match put_ticket {
            AnyPutTicket::Write(ticket) => {
                tracing::debug!(table = %ticket.table, batches = batches.len(), "do_put write");
                Self::execute_lance_write(self.conn.clone(), &ticket.table, batches).await?
            }
            AnyPutTicket::GraphWrite(ticket) => {
                let graph_uri = ticket
                    .graph_uri
                    .as_deref()
                    .or(self.graph_base_uri.as_deref())
                    .ok_or_else(|| {
                        Status::failed_precondition("no graph_base_uri configured")
                    })?;
                tracing::debug!(
                    target = %ticket.target,
                    graph_uri = %graph_uri,
                    batches = batches.len(),
                    "do_put graph_write"
                );
                Self::execute_graph_write(graph_uri, &ticket.target, batches).await?
            }
        };

        let result_meta = serde_json::json!({ "rows_written": row_count });
        let put_result = PutResult {
            app_metadata: serde_json::to_vec(&result_meta)
                .unwrap_or_default()
                .into(),
        };
        Ok(Response::new(Box::pin(futures::stream::once(async {
            Ok(put_result)
        }))))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange not supported"))
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();
        match action.r#type.as_str() {
            "cypher_mutate" => {
                let ticket: CypherMutateTicket =
                    serde_json::from_slice(&action.body).map_err(|e| {
                        Status::invalid_argument(format!("bad cypher_mutate body: {e}"))
                    })?;
                tracing::debug!(
                    cypher = %ticket.cypher,
                    params = ?ticket.params,
                    "do_action cypher_mutate"
                );
                let result =
                    Self::execute_cypher_mutate(ticket, self.graph_base_uri.as_deref()).await?;
                let body = serde_json::to_vec(&result).unwrap_or_default();
                let flight_result = arrow_flight::Result { body: body.into() };
                Ok(Response::new(Box::pin(futures::stream::once(async {
                    Ok(flight_result)
                }))))
            }
            other => Err(Status::unimplemented(format!("unknown action: {other}"))),
        }
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let actions = vec![Ok(ActionType {
            r#type: "cypher_mutate".into(),
            description: "Execute Cypher CREATE/MERGE/DELETE/SET with delta persistence".into(),
        })];
        Ok(Response::new(Box::pin(futures::stream::iter(actions))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_flight::decode::FlightRecordBatchStream;
    use arrow_schema::DataType;
    use futures::TryStreamExt;
    use indexmap::IndexMap;
    use yata_graph::LanceGraphStore;

    /// Write a simple 2-node 1-edge graph to a temp dir and verify Cypher round-trip.
    #[tokio::test]
    async fn test_execute_cypher_scan_arrow_ipc_shape() {
        let dir = tempfile::tempdir().unwrap();
        let graph_uri = dir.path().to_str().unwrap();

        // --- Write test graph to Lance ---
        let store = LanceGraphStore::new(graph_uri).await.unwrap();

        let mut alice_props = IndexMap::new();
        alice_props.insert("name".into(), yata_graph::json_to_cypher(&serde_json::json!("Alice")));
        let mut bob_props = IndexMap::new();
        bob_props.insert("name".into(), yata_graph::json_to_cypher(&serde_json::json!("Bob")));

        store.write_vertices(&[
            yata_cypher::NodeRef { id: "alice".into(), labels: vec!["Person".into()], props: alice_props },
            yata_cypher::NodeRef { id: "bob".into(),   labels: vec!["Person".into()], props: bob_props },
        ]).await.unwrap();

        store.write_edges(&[yata_cypher::RelRef {
            id:       "e1".into(),
            src:      "alice".into(),
            dst:      "bob".into(),
            rel_type: "KNOWS".into(),
            props:    IndexMap::new(),
        }]).await.unwrap();

        // --- execute_cypher_scan ---
        let ticket = CypherTicket {
            kind:      "cypher".into(),
            cypher:    "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS aname, b.name AS bname".into(),
            params:    vec![],
            graph_uri: Some(graph_uri.to_string()),
        };

        let flight_stream = YataFlightService::execute_cypher_scan(ticket, None)
            .await
            .unwrap();

        // Decode Arrow IPC from the flight stream.
        let decoded: Vec<RecordBatch> = FlightRecordBatchStream::new_from_flight_data(
            flight_stream.map_err(|e| arrow_flight::error::FlightError::Tonic(e)),
        )
        .try_collect()
        .await
        .unwrap();

        // Exactly 1 batch, 1 row.
        assert_eq!(decoded.len(), 1, "expected 1 record batch");
        let batch = &decoded[0];
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 2);

        // Column names come from RETURN clause aliases — transmitted once in schema.
        let schema = batch.schema();
        assert_eq!(schema.field(0).name(), "aname");
        assert_eq!(schema.field(1).name(), "bname");
        // All columns are Utf8 (JSON-encoded values).
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);

        // Row values.
        let col_a = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let col_b = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        // Values are JSON-encoded strings → `"Alice"` and `"Bob"`.
        assert_eq!(col_a.value(0), r#""Alice""#);
        assert_eq!(col_b.value(0), r#""Bob""#);
    }

    /// Empty Cypher result returns a zero-column schema stream (no panic).
    #[tokio::test]
    async fn test_execute_cypher_scan_empty_result() {
        let dir = tempfile::tempdir().unwrap();
        let graph_uri = dir.path().to_str().unwrap();

        // Empty graph — MATCH finds nothing.
        let ticket = CypherTicket {
            kind:      "cypher".into(),
            cypher:    "MATCH (n:Ghost) RETURN n.name".into(),
            params:    vec![],
            graph_uri: Some(graph_uri.to_string()),
        };

        let flight_stream = YataFlightService::execute_cypher_scan(ticket, None)
            .await
            .unwrap();

        let decoded: Vec<RecordBatch> = FlightRecordBatchStream::new_from_flight_data(
            flight_stream.map_err(|e| arrow_flight::error::FlightError::Tonic(e)),
        )
        .try_collect()
        .await
        .unwrap();

        // Empty graph → 0 columns in schema, 0 batches.
        assert!(decoded.is_empty() || decoded.iter().all(|b| b.num_rows() == 0));
    }

    /// QueryableGraph.query() rows align with cypher-result { columns, rows } encoding.
    #[tokio::test]
    async fn test_queryable_graph_column_row_alignment() {
        let dir = tempfile::tempdir().unwrap();
        let store = LanceGraphStore::new(dir.path().to_str().unwrap()).await.unwrap();

        let mut props = IndexMap::new();
        props.insert("age".into(), yata_cypher::Value::Int(25));
        props.insert("active".into(), yata_cypher::Value::Bool(true));

        store.write_vertices(&[
            yata_cypher::NodeRef { id: "u1".into(), labels: vec!["User".into()], props },
        ]).await.unwrap();

        let mut g = store.to_memory_graph().await.unwrap();
        let rows = g.query(
            "MATCH (u:User) RETURN u.age AS age, u.active AS active",
            &[],
        ).unwrap();

        assert_eq!(rows.len(), 1);
        // cypher-result layout: columns[0]="age", columns[1]="active"; rows[0][0] / rows[0][1]
        let cols: Vec<&str> = rows[0].iter().map(|(c, _)| c.as_str()).collect();
        assert!(cols.contains(&"age"),    "missing age column");
        assert!(cols.contains(&"active"), "missing active column");

        let age_val = rows[0].iter().find(|(c, _)| c == "age").map(|(_, v)| v.as_str()).unwrap_or("");
        let active_val = rows[0].iter().find(|(c, _)| c == "active").map(|(_, v)| v.as_str()).unwrap_or("");
        assert_eq!(age_val,    "25");
        assert_eq!(active_val, "true");
    }

    // ---- Write operation tests ------------------------------------------

    /// Lance table direct write: build RecordBatch → execute_lance_write → verify via scan.
    #[tokio::test]
    async fn test_execute_lance_write_and_scan_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let conn = lancedb::connect(uri).execute().await.unwrap();

        // Build a 2-row batch
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["r1", "r2"])) as Arc<dyn arrow_array::Array>,
                Arc::new(StringArray::from(vec!["hello", "world"])) as Arc<dyn arrow_array::Array>,
            ],
        )
        .unwrap();

        let rows = YataFlightService::execute_lance_write(conn.clone(), "test_table", vec![batch])
            .await
            .unwrap();
        assert_eq!(rows, 2);

        // Read back
        let tbl = conn.open_table("test_table").execute().await.unwrap();
        let batches: Vec<RecordBatch> = tbl
            .query()
            .execute()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    /// Graph vertex batch write: build Arrow RecordBatch → write_vertices_batch → load back.
    #[tokio::test]
    async fn test_execute_graph_write_vertices_batch() {
        let dir = tempfile::tempdir().unwrap();
        let graph_uri = dir.path().to_str().unwrap();

        let schema = yata_graph::graph_arrow::vertices_schema();
        let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["v1", "v2"])) as Arc<dyn arrow_array::Array>,
                Arc::new(StringArray::from(vec![r#"["Person"]"#, r#"["Company"]"#]))
                    as Arc<dyn arrow_array::Array>,
                Arc::new(StringArray::from(vec![
                    r#"{"name":"Alice"}"#,
                    r#"{"name":"GFTD"}"#,
                ])) as Arc<dyn arrow_array::Array>,
                Arc::new(Int64Array::from(vec![now_ns, now_ns])) as Arc<dyn arrow_array::Array>,
            ],
        )
        .unwrap();

        let rows =
            YataFlightService::execute_graph_write(graph_uri, "vertices", vec![batch])
                .await
                .unwrap();
        assert_eq!(rows, 2);

        // Verify via LanceGraphStore load
        let store = LanceGraphStore::new(graph_uri).await.unwrap();
        let nodes = store.load_vertices().await.unwrap();
        assert_eq!(nodes.len(), 2);
        let ids: Vec<&str> = nodes.iter().map(|n| n.id.as_str()).collect();
        assert!(ids.contains(&"v1"));
        assert!(ids.contains(&"v2"));
    }

    /// Graph edge batch write: Arrow RecordBatch → write_edges_batch → verify adj auto-gen.
    #[tokio::test]
    async fn test_execute_graph_write_edges_batch_with_adj() {
        let dir = tempfile::tempdir().unwrap();
        let graph_uri = dir.path().to_str().unwrap();

        // Write vertices first
        let store = LanceGraphStore::new(graph_uri).await.unwrap();
        store
            .write_vertices(&[
                yata_cypher::NodeRef {
                    id: "a".into(),
                    labels: vec!["X".into()],
                    props: IndexMap::new(),
                },
                yata_cypher::NodeRef {
                    id: "b".into(),
                    labels: vec!["Y".into()],
                    props: IndexMap::new(),
                },
            ])
            .await
            .unwrap();

        // Build edge batch
        let edge_schema = yata_graph::graph_arrow::edges_schema();
        let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let edge_batch = RecordBatch::try_new(
            edge_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["e1"])) as Arc<dyn arrow_array::Array>,
                Arc::new(StringArray::from(vec!["a"])) as Arc<dyn arrow_array::Array>,
                Arc::new(StringArray::from(vec!["b"])) as Arc<dyn arrow_array::Array>,
                Arc::new(StringArray::from(vec!["LINKS"])) as Arc<dyn arrow_array::Array>,
                Arc::new(StringArray::from(vec![r#"{}"#])) as Arc<dyn arrow_array::Array>,
                Arc::new(Int64Array::from(vec![now_ns])) as Arc<dyn arrow_array::Array>,
            ],
        )
        .unwrap();

        let rows =
            YataFlightService::execute_graph_write(graph_uri, "edges", vec![edge_batch])
                .await
                .unwrap();
        assert_eq!(rows, 1);

        // Verify edges loaded
        let edges = store.load_edges().await.unwrap();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].rel_type, "LINKS");

        // Verify Cypher traversal works (adj index must be present)
        let mut g = store.to_memory_graph().await.unwrap();
        let result = g
            .query("MATCH (a)-[:LINKS]->(b) RETURN a, b", &[])
            .unwrap();
        assert_eq!(result.len(), 1);
    }

    /// Cypher mutate: CREATE nodes → verify delta persisted to Lance.
    #[tokio::test]
    async fn test_execute_cypher_mutate_create() {
        let dir = tempfile::tempdir().unwrap();
        let graph_uri = dir.path().to_str().unwrap();

        let ticket = CypherMutateTicket::new(
            "CREATE (a:Person {name: \"Alice\"})-[:KNOWS]->(b:Person {name: \"Bob\"})",
            vec![],
        );
        let mut ticket_with_uri = ticket;
        ticket_with_uri.graph_uri = Some(graph_uri.to_string());

        let result =
            YataFlightService::execute_cypher_mutate(ticket_with_uri, None)
                .await
                .unwrap();

        assert_eq!(result.nodes_created, 2);
        assert_eq!(result.edges_created, 1);
        assert_eq!(result.nodes_modified, 0);
        assert_eq!(result.edges_modified, 0);
        assert_eq!(result.nodes_deleted, 0);
        assert_eq!(result.edges_deleted, 0);

        // Verify persistence: reload and query
        let store = LanceGraphStore::new(graph_uri).await.unwrap();
        let mut g = store.to_memory_graph().await.unwrap();
        let rows = g
            .query(
                "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS aname, b.name AS bname",
                &[],
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
    }

    /// Cypher mutate: SET property on existing node → verify modification persisted.
    #[tokio::test]
    async fn test_execute_cypher_mutate_set() {
        let dir = tempfile::tempdir().unwrap();
        let graph_uri = dir.path().to_str().unwrap();

        // Seed graph
        let store = LanceGraphStore::new(graph_uri).await.unwrap();
        let mut props = IndexMap::new();
        props.insert("name".into(), yata_cypher::Value::Str("Alice".into()));
        props.insert("age".into(), yata_cypher::Value::Int(30));
        store
            .write_vertices(&[yata_cypher::NodeRef {
                id: "alice".into(),
                labels: vec!["Person".into()],
                props,
            }])
            .await
            .unwrap();

        // Mutate: SET age = 31
        let ticket = CypherMutateTicket {
            cypher: "MATCH (n:Person {name: \"Alice\"}) SET n.age = 31 RETURN n".into(),
            params: vec![],
            graph_uri: Some(graph_uri.to_string()),
        };

        let result = YataFlightService::execute_cypher_mutate(ticket, None)
            .await
            .unwrap();
        assert_eq!(result.nodes_modified, 1);
        assert_eq!(result.nodes_created, 0);

        // Verify persistence
        let store2 = LanceGraphStore::new(graph_uri).await.unwrap();
        let mut g = store2.to_memory_graph().await.unwrap();
        let rows = g.query("MATCH (n:Person) RETURN n.age AS age", &[]).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].1, "31");
    }

    /// Cypher mutate: DETACH DELETE → verify tombstone written, node absent on reload.
    #[tokio::test]
    async fn test_execute_cypher_mutate_delete() {
        let dir = tempfile::tempdir().unwrap();
        let graph_uri = dir.path().to_str().unwrap();

        // Seed: Alice -[KNOWS]-> Bob
        let store = LanceGraphStore::new(graph_uri).await.unwrap();
        let mut ap = IndexMap::new();
        ap.insert("name".into(), yata_cypher::Value::Str("Alice".into()));
        let mut bp = IndexMap::new();
        bp.insert("name".into(), yata_cypher::Value::Str("Bob".into()));
        store
            .write_vertices(&[
                yata_cypher::NodeRef { id: "alice".into(), labels: vec!["Person".into()], props: ap },
                yata_cypher::NodeRef { id: "bob".into(),   labels: vec!["Person".into()], props: bp },
            ])
            .await
            .unwrap();
        store
            .write_edges(&[yata_cypher::RelRef {
                id: "e1".into(), src: "alice".into(), dst: "bob".into(),
                rel_type: "KNOWS".into(), props: IndexMap::new(),
            }])
            .await
            .unwrap();

        // Delete Alice (detach)
        let ticket = CypherMutateTicket {
            cypher: "MATCH (n:Person {name: \"Alice\"}) DETACH DELETE n".into(),
            params: vec![],
            graph_uri: Some(graph_uri.to_string()),
        };
        let result = YataFlightService::execute_cypher_mutate(ticket, None)
            .await
            .unwrap();
        assert_eq!(result.nodes_deleted, 1);
        assert_eq!(result.edges_deleted, 1);

        // Verify: reload — last-write-wins with _deleted tombstone.
        // The tombstone overwrites the original node (props contains _deleted:true).
        let store2 = LanceGraphStore::new(graph_uri).await.unwrap();
        let nodes = store2.load_vertices().await.unwrap();
        // alice should have _deleted:true in props
        let alice = nodes.iter().find(|n| n.id == "alice");
        assert!(alice.is_some());
        let has_deleted = alice
            .unwrap()
            .props
            .get("_deleted")
            .map(|v| matches!(v, yata_cypher::Value::Bool(true)))
            .unwrap_or(false);
        assert!(has_deleted, "alice should have _deleted tombstone");
    }

    /// Multi-batch do_put: two batches written atomically in one Lance append.
    #[tokio::test]
    async fn test_execute_lance_write_multi_batch_concat() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let conn = lancedb::connect(uri).execute().await.unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
        ]));
        let b1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["a", "b"])) as Arc<dyn arrow_array::Array>],
        )
        .unwrap();
        let b2 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["c"])) as Arc<dyn arrow_array::Array>],
        )
        .unwrap();

        let rows =
            YataFlightService::execute_lance_write(conn.clone(), "multi", vec![b1, b2])
                .await
                .unwrap();
        assert_eq!(rows, 3);

        let tbl = conn.open_table("multi").execute().await.unwrap();
        let batches: Vec<RecordBatch> = tbl
            .query()
            .execute()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3);
    }
}
