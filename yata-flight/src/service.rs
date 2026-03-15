use std::pin::Pin;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow_array::StringArray;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
    encode::FlightDataEncoderBuilder,
    error::{FlightError, Result as FlightResult},
    flight_service_server::FlightService,
};
use arrow_schema::{DataType, Field, Schema};
use futures::{Stream, TryStreamExt};
use tonic::{Request, Response, Status, Streaming};

use crate::catalog::YataTableCatalog;
use crate::codec::{AnyTicket, CypherTicket, ScanTicket};

pub struct YataFlightService {
    catalog: Arc<YataTableCatalog>,
    /// Base URI for graph_vertices / graph_edges Lance tables.
    /// When set, enables Cypher queries via `CypherTicket`.
    graph_base_uri: Option<String>,
}

impl YataFlightService {
    pub fn new(lance_base_uri: impl Into<String>) -> Self {
        Self {
            catalog: Arc::new(YataTableCatalog::new(lance_base_uri)),
            graph_base_uri: None,
        }
    }

    /// Create service with graph store support for Cypher queries.
    pub fn new_with_graph(
        lance_base_uri: impl Into<String>,
        graph_base_uri: impl Into<String>,
    ) -> Self {
        Self {
            catalog: Arc::new(YataTableCatalog::new(lance_base_uri)),
            graph_base_uri: Some(graph_base_uri.into()),
        }
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
        ticket: ScanTicket,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>, Status>
    {
        let schema = catalog
            .schema(&ticket.table)
            .ok_or_else(|| Status::not_found(format!("table not found: {}", ticket.table)))?;

        let uri = catalog.dataset_uri(&ticket.table);

        let dataset = match lance::dataset::Dataset::open(&uri).await {
            Ok(ds) => ds,
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

        let mut scanner = dataset.scan();

        if let Some(filter) = &ticket.filter {
            scanner
                .filter(filter)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
        }
        if let Some(proj) = &ticket.projection {
            let cols: Vec<&str> = proj.iter().map(|s| s.as_str()).collect();
            scanner
                .project(&cols)
                .map_err(|e| Status::internal(e.to_string()))?;
        }
        if ticket.limit.is_some() || ticket.offset.is_some() {
            scanner
                .limit(ticket.limit, ticket.offset)
                .map_err(|e| Status::internal(e.to_string()))?;
        }

        let batch_stream = scanner
            .try_into_stream()
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
                let stream = Self::execute_scan(self.catalog.clone(), ticket).await?;
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
        }
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put not supported"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange not supported"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action not supported"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Ok(Response::new(Box::pin(futures::stream::empty())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
}
