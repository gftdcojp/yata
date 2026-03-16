use std::pin::Pin;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow_array::StringArray;
use arrow_flight::{
    Action, ActionType, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, Ticket,
    decode::FlightRecordBatchStream,
    encode::FlightDataEncoderBuilder,
    error::{FlightError, Result as FlightResult},
    flight_service_server::FlightService,
    sql::{
        Any, CommandGetCatalogs, CommandGetCrossReference, CommandGetDbSchemas,
        CommandGetExportedKeys, CommandGetImportedKeys, CommandGetPrimaryKeys, CommandGetSqlInfo,
        CommandGetTableTypes, CommandGetTables, CommandGetXdbcTypeInfo,
        CommandPreparedStatementQuery, CommandStatementQuery, CommandStatementUpdate,
        ProstMessageExt, SqlInfo, SqlSupportedTransaction, TicketStatementQuery,
        metadata::{
            GetCatalogsBuilder, SqlInfoData, SqlInfoDataBuilder,
        },
        server::{FlightSqlService, PeekableFlightDataStream},
    },
};
use arrow_schema::{DataType, Field, Schema};
use futures::{Stream, TryStreamExt};
use indexmap::IndexMap;
use lancedb::query::{ExecutableQuery, QueryBase};
use once_cell::sync::Lazy;
use prost::Message;
use tonic::{Request, Response, Status, Streaming};

use crate::catalog::YataTableCatalog;
use crate::codec::{
    AnyPutTicket, AnyTicket, CypherMutateResult, CypherMutateTicket, CypherTicket, ScanTicket,
    VectorSearchTicket,
};

static SQL_INFO: Lazy<SqlInfoData> = Lazy::new(|| {
    let mut builder = SqlInfoDataBuilder::new();
    builder.append(SqlInfo::FlightSqlServerName, "yata-flight");
    builder.append(SqlInfo::FlightSqlServerVersion, env!("CARGO_PKG_VERSION"));
    builder.append(SqlInfo::FlightSqlServerArrowVersion, "1.3");
    builder.append(SqlInfo::FlightSqlServerReadOnly, false);
    builder.append(
        SqlInfo::FlightSqlServerTransaction,
        SqlSupportedTransaction::None as i32,
    );
    builder.append(SqlInfo::FlightSqlServerSql, false);
    builder.build().expect("static SqlInfoData")
});

pub struct YataFlightService {
    catalog: Arc<YataTableCatalog>,
    conn: lancedb::Connection,
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

    fn make_flight_info_for_cmd(cmd: &impl ProstMessageExt) -> Result<FlightInfo, Status> {
        let any = cmd.as_any();
        let ticket = Ticket::new(any.encode_to_vec());
        Ok(FlightInfo::new()
            .with_endpoint(FlightEndpoint::new().with_ticket(ticket))
            .with_total_records(-1)
            .with_total_bytes(-1))
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
            let schema = Arc::new(Schema::empty());
            let empty = futures::stream::empty::<FlightResult<RecordBatch>>();
            let stream = FlightDataEncoderBuilder::new()
                .with_schema(schema)
                .build(empty)
                .map_err(tonic::Status::from);
            return Ok(Box::pin(stream));
        }

        let col_names: Vec<String> = raw_rows[0].iter().map(|(name, _)| name.clone()).collect();
        let fields: Vec<Field> = col_names
            .iter()
            .map(|name| Field::new(name.as_str(), DataType::Utf8, true))
            .collect();
        let schema = Arc::new(Schema::new(fields));

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

    fn concat_batches(batches: &[RecordBatch]) -> Result<RecordBatch, Status> {
        if batches.is_empty() {
            return Err(Status::invalid_argument("no record batches in put stream"));
        }
        arrow::compute::concat_batches(&batches[0].schema(), batches)
            .map_err(|e| Status::internal(format!("concat batches: {e}")))
    }

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

    async fn collect_peekable_batches(
        stream: PeekableFlightDataStream,
    ) -> Result<Vec<RecordBatch>, Status> {
        let flight_stream = FlightRecordBatchStream::new_from_flight_data(
            stream.map_err(|e| FlightError::Tonic(e)),
        );
        flight_stream
            .try_collect()
            .await
            .map_err(|e| Status::internal(format!("decode flight data: {e}")))
    }
}

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

// ---------------------------------------------------------------------------
// FlightSqlService implementation
// ---------------------------------------------------------------------------

#[tonic::async_trait]
impl FlightSqlService for YataFlightService {
    type FlightService = YataFlightService;

    // ---- Handshake --------------------------------------------------------

    async fn do_handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        let response = HandshakeResponse {
            protocol_version: 0,
            payload: bytes::Bytes::new(),
        };
        let stream = futures::stream::once(async { Ok(response) });
        Ok(Response::new(Box::pin(stream)))
    }

    // ---- Statement Query (Cypher) -----------------------------------------

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        yata_cypher::parse(&query.query)
            .map_err(|e| Status::invalid_argument(format!("cypher parse: {e}")))?;

        let handle = query.query.clone().into_bytes();
        let ticket = TicketStatementQuery {
            statement_handle: handle.into(),
        };
        let info = Self::make_flight_info_for_cmd(&ticket)?;
        Ok(Response::new(info))
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let cypher = String::from_utf8(ticket.statement_handle.to_vec())
            .map_err(|_| Status::invalid_argument("invalid statement handle"))?;

        let cypher_ticket = CypherTicket::new(cypher, vec![]);
        let stream =
            Self::execute_cypher_scan(cypher_ticket, self.graph_base_uri.as_deref()).await?;
        Ok(Response::new(stream))
    }

    // ---- Statement Update (Cypher mutation) --------------------------------

    async fn do_put_statement_update(
        &self,
        ticket: CommandStatementUpdate,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        let mutate_ticket = CypherMutateTicket::new(ticket.query, vec![]);
        let result =
            Self::execute_cypher_mutate(mutate_ticket, self.graph_base_uri.as_deref()).await?;
        let affected = (result.nodes_created
            + result.nodes_modified
            + result.nodes_deleted
            + result.edges_created
            + result.edges_modified
            + result.edges_deleted) as i64;
        Ok(affected)
    }

    // ---- Prepared Statements (stateless) ----------------------------------

    async fn do_get_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let cypher = String::from_utf8(query.prepared_statement_handle.to_vec())
            .map_err(|_| Status::invalid_argument("invalid prepared statement handle"))?;
        let cypher_ticket = CypherTicket::new(cypher, vec![]);
        let stream =
            Self::execute_cypher_scan(cypher_ticket, self.graph_base_uri.as_deref()).await?;
        Ok(Response::new(stream))
    }

    async fn get_flight_info_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let info = Self::make_flight_info_for_cmd(&query)?;
        Ok(Response::new(info))
    }

    async fn do_action_create_prepared_statement(
        &self,
        query: arrow_flight::sql::ActionCreatePreparedStatementRequest,
        _request: Request<Action>,
    ) -> Result<arrow_flight::sql::ActionCreatePreparedStatementResult, Status> {
        yata_cypher::parse(&query.query)
            .map_err(|e| Status::invalid_argument(format!("cypher parse: {e}")))?;

        Ok(arrow_flight::sql::ActionCreatePreparedStatementResult {
            prepared_statement_handle: query.query.into_bytes().into(),
            dataset_schema: bytes::Bytes::new(),
            parameter_schema: bytes::Bytes::new(),
        })
    }

    async fn do_action_close_prepared_statement(
        &self,
        _query: arrow_flight::sql::ActionClosePreparedStatementRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        Ok(())
    }

    // ---- Catalogs ---------------------------------------------------------

    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Self::make_flight_info_for_cmd(&query).map(Response::new)
    }

    async fn do_get_catalogs(
        &self,
        _query: CommandGetCatalogs,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let mut builder = GetCatalogsBuilder::new();
        builder.append("yata");
        let batch = builder.build().map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(Self::batch_to_stream(batch)))
    }

    // ---- DB Schemas -------------------------------------------------------

    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Self::make_flight_info_for_cmd(&query).map(Response::new)
    }

    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let mut builder = query.into_builder();
        builder.append("yata", "public");
        let batch = builder.build().map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(Self::batch_to_stream(batch)))
    }

    // ---- Tables -----------------------------------------------------------

    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Self::make_flight_info_for_cmd(&query).map(Response::new)
    }

    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let mut builder = query.into_builder();

        for &table_name in self.catalog.tables() {
            let schema = self
                .catalog
                .schema(table_name)
                .unwrap_or_else(|| Arc::new(Schema::empty()));
            builder
                .append("yata", "public", table_name, "TABLE", &schema)
                .map_err(|e| Status::internal(e.to_string()))?;
        }

        if self.graph_base_uri.is_some() {
            let graph_tables = [
                ("graph_vertices", yata_graph::graph_arrow::vertices_schema()),
                ("graph_edges", yata_graph::graph_arrow::edges_schema()),
                ("graph_adj", yata_graph::graph_arrow::adj_schema()),
            ];
            for (name, schema) in &graph_tables {
                builder
                    .append("yata", "public", *name, "TABLE", schema)
                    .map_err(|e| Status::internal(e.to_string()))?;
            }
        }

        let batch = builder.build().map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(Self::batch_to_stream(batch)))
    }

    // ---- Table Types ------------------------------------------------------

    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Self::make_flight_info_for_cmd(&query).map(Response::new)
    }

    async fn do_get_table_types(
        &self,
        _query: CommandGetTableTypes,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "table_type",
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["TABLE"])) as _],
        )
        .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(Self::batch_to_stream(batch)))
    }

    // ---- SqlInfo ----------------------------------------------------------

    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Self::make_flight_info_for_cmd(&query).map(Response::new)
    }

    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let info: &SqlInfoData = &SQL_INFO;
        let ids: Vec<u32> = query.info.into_iter().map(|v| v as u32).collect();
        let batch = info
            .record_batch(ids)
            .map_err(|e: arrow_flight::error::FlightError| Status::internal(e.to_string()))?;
        Ok(Response::new(Self::batch_to_stream(batch)))
    }

    // ---- Primary Keys (empty — Lance has none) ----------------------------

    async fn get_flight_info_primary_keys(
        &self,
        query: CommandGetPrimaryKeys,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Self::make_flight_info_for_cmd(&query).map(Response::new)
    }

    async fn do_get_primary_keys(
        &self,
        _query: CommandGetPrimaryKeys,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, true),
            Field::new("db_schema_name", DataType::Utf8, true),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("column_name", DataType::Utf8, false),
            Field::new("key_sequence", DataType::Int32, false),
            Field::new("key_name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::new_empty(schema);
        Ok(Response::new(Self::batch_to_stream(batch)))
    }

    // ---- Exported/Imported Keys & Cross Reference (empty) -----------------

    async fn get_flight_info_exported_keys(
        &self,
        query: CommandGetExportedKeys,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Self::make_flight_info_for_cmd(&query).map(Response::new)
    }

    async fn do_get_exported_keys(
        &self,
        _query: CommandGetExportedKeys,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Ok(Response::new(Self::empty_keys_stream()))
    }

    async fn get_flight_info_imported_keys(
        &self,
        query: CommandGetImportedKeys,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Self::make_flight_info_for_cmd(&query).map(Response::new)
    }

    async fn do_get_imported_keys(
        &self,
        _query: CommandGetImportedKeys,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Ok(Response::new(Self::empty_keys_stream()))
    }

    async fn get_flight_info_cross_reference(
        &self,
        query: CommandGetCrossReference,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Self::make_flight_info_for_cmd(&query).map(Response::new)
    }

    async fn do_get_cross_reference(
        &self,
        _query: CommandGetCrossReference,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Ok(Response::new(Self::empty_keys_stream()))
    }

    // ---- XDBC Type Info ---------------------------------------------------

    async fn get_flight_info_xdbc_type_info(
        &self,
        query: CommandGetXdbcTypeInfo,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Self::make_flight_info_for_cmd(&query).map(Response::new)
    }

    async fn do_get_xdbc_type_info(
        &self,
        _query: CommandGetXdbcTypeInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        use arrow_flight::sql::metadata::XdbcTypeInfoDataBuilder;
        let builder = XdbcTypeInfoDataBuilder::new();
        let info_data = builder.build().map_err(|e| Status::internal(e.to_string()))?;
        let batch = info_data
            .record_batch(None::<i32>)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(Self::batch_to_stream(batch)))
    }

    // ---- Fallback for custom tickets --------------------------------------

    async fn do_get_fallback(
        &self,
        request: Request<Ticket>,
        message: Any,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        // Try to decode custom yata tickets from the Any value field or raw ticket bytes.
        let ticket_bytes = if !message.type_url.is_empty()
            && message.type_url.starts_with("type.yata.io/")
        {
            &message.value[..]
        } else {
            &request.get_ref().ticket[..]
        };

        if let Ok(any_ticket) = AnyTicket::from_bytes(ticket_bytes) {
            return match any_ticket {
                AnyTicket::Scan(ticket) => {
                    let stream =
                        Self::execute_scan(self.catalog.clone(), self.conn.clone(), ticket).await?;
                    Ok(Response::new(stream))
                }
                AnyTicket::Cypher(ticket) => {
                    let stream =
                        Self::execute_cypher_scan(ticket, self.graph_base_uri.as_deref()).await?;
                    Ok(Response::new(stream))
                }
                AnyTicket::VectorSearch(ticket) => {
                    let stream =
                        Self::execute_vector_search(self.conn.clone(), ticket).await?;
                    Ok(Response::new(stream))
                }
            };
        }

        Err(Status::unimplemented(format!(
            "do_get: unknown ticket type: {}",
            message.type_url
        )))
    }

    // ---- do_put fallback for custom write tickets -------------------------

    async fn do_put_fallback(
        &self,
        request: Request<PeekableFlightDataStream>,
        message: Any,
    ) -> Result<Response<<Self as FlightService>::DoPutStream>, Status> {
        let put_ticket = AnyPutTicket::from_bytes(&message.value)
            .map_err(|e| Status::invalid_argument(format!("invalid put ticket: {e}")))?;

        let batches = Self::collect_peekable_batches(request.into_inner()).await?;

        let row_count = match put_ticket {
            AnyPutTicket::Write(ticket) => {
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

    // ---- do_action fallback (cypher_mutate) --------------------------------

    async fn do_action_fallback(
        &self,
        request: Request<Action>,
    ) -> Result<Response<<Self as FlightService>::DoActionStream>, Status> {
        let action = request.into_inner();
        match action.r#type.as_str() {
            "cypher_mutate" => {
                let ticket: CypherMutateTicket =
                    serde_json::from_slice(&action.body).map_err(|e| {
                        Status::invalid_argument(format!("bad cypher_mutate body: {e}"))
                    })?;
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

    async fn list_custom_actions(&self) -> Option<Vec<Result<ActionType, Status>>> {
        Some(vec![Ok(ActionType {
            r#type: "cypher_mutate".into(),
            description: "Execute Cypher CREATE/MERGE/DELETE/SET with delta persistence".into(),
        })])
    }

    // ---- Substrait (not supported — Cypher-based) --------------------------

    async fn get_flight_info_substrait_plan(
        &self,
        _query: arrow_flight::sql::CommandStatementSubstraitPlan,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "Substrait plans are not supported; use Cypher via CommandStatementQuery",
        ))
    }

    async fn do_put_substrait_plan(
        &self,
        _query: arrow_flight::sql::CommandStatementSubstraitPlan,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        Err(Status::unimplemented(
            "Substrait plans are not supported; use Cypher via CommandStatementUpdate",
        ))
    }

    async fn do_action_create_prepared_substrait_plan(
        &self,
        _query: arrow_flight::sql::ActionCreatePreparedSubstraitPlanRequest,
        _request: Request<Action>,
    ) -> Result<arrow_flight::sql::ActionCreatePreparedStatementResult, Status> {
        Err(Status::unimplemented(
            "Substrait plans are not supported; use CreatePreparedStatement with Cypher",
        ))
    }

    // ---- Transactions (not supported — Lance is append-only) ---------------

    async fn do_action_begin_transaction(
        &self,
        _query: arrow_flight::sql::ActionBeginTransactionRequest,
        _request: Request<Action>,
    ) -> Result<arrow_flight::sql::ActionBeginTransactionResult, Status> {
        Err(Status::unimplemented(
            "Transactions are not supported; Lance storage is append-only",
        ))
    }

    async fn do_action_end_transaction(
        &self,
        _query: arrow_flight::sql::ActionEndTransactionRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        Err(Status::unimplemented(
            "Transactions are not supported; Lance storage is append-only",
        ))
    }

    async fn do_action_begin_savepoint(
        &self,
        _query: arrow_flight::sql::ActionBeginSavepointRequest,
        _request: Request<Action>,
    ) -> Result<arrow_flight::sql::ActionBeginSavepointResult, Status> {
        Err(Status::unimplemented(
            "Savepoints are not supported; Lance storage is append-only",
        ))
    }

    async fn do_action_end_savepoint(
        &self,
        _query: arrow_flight::sql::ActionEndSavepointRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        Err(Status::unimplemented(
            "Savepoints are not supported; Lance storage is append-only",
        ))
    }

    // ---- Cancel query (not supported — synchronous execution) --------------

    async fn do_action_cancel_query(
        &self,
        _query: arrow_flight::sql::ActionCancelQueryRequest,
        _request: Request<Action>,
    ) -> Result<arrow_flight::sql::ActionCancelQueryResult, Status> {
        Err(Status::unimplemented(
            "Cancel query is not supported; queries execute synchronously",
        ))
    }

    // ---- Prepared statement bind/update (not supported — stateless) --------

    async fn do_put_prepared_statement_query(
        &self,
        _query: CommandPreparedStatementQuery,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<arrow_flight::sql::DoPutPreparedStatementResult, Status> {
        Err(Status::unimplemented(
            "Parameter binding is not supported; prepared statements are stateless",
        ))
    }

    async fn do_put_prepared_statement_update(
        &self,
        _query: arrow_flight::sql::CommandPreparedStatementUpdate,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        Err(Status::unimplemented(
            "Parameter binding is not supported; prepared statements are stateless",
        ))
    }

    // ---- Bulk ingest (not supported) ---------------------------------------

    async fn do_put_statement_ingest(
        &self,
        _ticket: arrow_flight::sql::CommandStatementIngest,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        Err(Status::unimplemented(
            "Bulk ingest is not supported; use do_put with WriteTicket",
        ))
    }

    // ---- do_exchange (not supported) ---------------------------------------

    async fn do_exchange_fallback(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<<Self as FlightService>::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange is not supported"))
    }

    // ---- get_flight_info_fallback ------------------------------------------

    async fn get_flight_info_fallback(
        &self,
        cmd: arrow_flight::sql::Command,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(format!(
            "get_flight_info: unsupported command: {}",
            cmd.type_url()
        )))
    }

    // ---- register_sql_info ------------------------------------------------

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {
        // Static SqlInfoData — registration is a no-op.
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

impl YataFlightService {
    fn batch_to_stream(batch: RecordBatch) -> BoxStream<FlightData> {
        let schema = batch.schema();
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::once(async move {
                Ok::<_, FlightError>(batch)
            }))
            .map_err(tonic::Status::from);
        Box::pin(stream)
    }

    fn empty_keys_stream() -> BoxStream<FlightData> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk_catalog_name", DataType::Utf8, true),
            Field::new("pk_db_schema_name", DataType::Utf8, true),
            Field::new("pk_table_name", DataType::Utf8, false),
            Field::new("pk_column_name", DataType::Utf8, false),
            Field::new("fk_catalog_name", DataType::Utf8, true),
            Field::new("fk_db_schema_name", DataType::Utf8, true),
            Field::new("fk_table_name", DataType::Utf8, false),
            Field::new("fk_column_name", DataType::Utf8, false),
            Field::new("key_sequence", DataType::Int32, false),
            Field::new("fk_key_name", DataType::Utf8, true),
            Field::new("pk_key_name", DataType::Utf8, true),
            Field::new("update_rule", DataType::UInt8, false),
            Field::new("delete_rule", DataType::UInt8, false),
        ]));
        let batch = RecordBatch::new_empty(schema.clone());
        Self::batch_to_stream(batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, Int64Array};
    use arrow_flight::decode::FlightRecordBatchStream;
    use arrow_schema::DataType;
    use futures::TryStreamExt;
    use indexmap::IndexMap;
    use yata_graph::LanceGraphStore;

    #[tokio::test]
    async fn test_execute_cypher_scan_arrow_ipc_shape() {
        let dir = tempfile::tempdir().unwrap();
        let graph_uri = dir.path().to_str().unwrap();

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

        let ticket = CypherTicket {
            kind:      "cypher".into(),
            cypher:    "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS aname, b.name AS bname".into(),
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

        assert_eq!(decoded.len(), 1, "expected 1 record batch");
        let batch = &decoded[0];
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 2);

        let schema = batch.schema();
        assert_eq!(schema.field(0).name(), "aname");
        assert_eq!(schema.field(1).name(), "bname");
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);

        let col_a = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let col_b = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(col_a.value(0), r#""Alice""#);
        assert_eq!(col_b.value(0), r#""Bob""#);
    }

    #[tokio::test]
    async fn test_execute_cypher_scan_empty_result() {
        let dir = tempfile::tempdir().unwrap();
        let graph_uri = dir.path().to_str().unwrap();

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

        assert!(decoded.is_empty() || decoded.iter().all(|b| b.num_rows() == 0));
    }

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
        let cols: Vec<&str> = rows[0].iter().map(|(c, _)| c.as_str()).collect();
        assert!(cols.contains(&"age"),    "missing age column");
        assert!(cols.contains(&"active"), "missing active column");

        let age_val = rows[0].iter().find(|(c, _)| c == "age").map(|(_, v)| v.as_str()).unwrap_or("");
        let active_val = rows[0].iter().find(|(c, _)| c == "active").map(|(_, v)| v.as_str()).unwrap_or("");
        assert_eq!(age_val,    "25");
        assert_eq!(active_val, "true");
    }

    #[tokio::test]
    async fn test_execute_lance_write_and_scan_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let conn = lancedb::connect(uri).execute().await.unwrap();

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

        let store = LanceGraphStore::new(graph_uri).await.unwrap();
        let nodes = store.load_vertices().await.unwrap();
        assert_eq!(nodes.len(), 2);
        let ids: Vec<&str> = nodes.iter().map(|n| n.id.as_str()).collect();
        assert!(ids.contains(&"v1"));
        assert!(ids.contains(&"v2"));
    }

    #[tokio::test]
    async fn test_execute_graph_write_edges_batch_with_adj() {
        let dir = tempfile::tempdir().unwrap();
        let graph_uri = dir.path().to_str().unwrap();

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

        let edges = store.load_edges().await.unwrap();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].rel_type, "LINKS");

        let mut g = store.to_memory_graph().await.unwrap();
        let result = g
            .query("MATCH (a)-[:LINKS]->(b) RETURN a, b", &[])
            .unwrap();
        assert_eq!(result.len(), 1);
    }

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

    #[tokio::test]
    async fn test_execute_cypher_mutate_set() {
        let dir = tempfile::tempdir().unwrap();
        let graph_uri = dir.path().to_str().unwrap();

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

        let store2 = LanceGraphStore::new(graph_uri).await.unwrap();
        let mut g = store2.to_memory_graph().await.unwrap();
        let rows = g.query("MATCH (n:Person) RETURN n.age AS age", &[]).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].1, "31");
    }

    #[tokio::test]
    async fn test_execute_cypher_mutate_delete() {
        let dir = tempfile::tempdir().unwrap();
        let graph_uri = dir.path().to_str().unwrap();

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

        let store2 = LanceGraphStore::new(graph_uri).await.unwrap();
        let nodes = store2.load_vertices().await.unwrap();
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

    // ---- Flight SQL protocol tests ----------------------------------------

    #[tokio::test]
    async fn test_flight_sql_catalogs() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let svc = YataFlightService::new(uri).await.unwrap();

        let cmd = CommandGetCatalogs {};
        let resp = svc
            .do_get_catalogs(cmd, Request::new(Ticket::new(bytes::Bytes::new())))
            .await
            .unwrap();

        let batches: Vec<RecordBatch> = FlightRecordBatchStream::new_from_flight_data(
            resp.into_inner().map_err(FlightError::Tonic),
        )
        .try_collect()
        .await
        .unwrap();

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 1);
        let col = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(col.value(0), "yata");
    }

    #[tokio::test]
    async fn test_flight_sql_schemas() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let svc = YataFlightService::new(uri).await.unwrap();

        let cmd = CommandGetDbSchemas {
            catalog: None,
            db_schema_filter_pattern: None,
        };
        let resp = svc
            .do_get_schemas(cmd, Request::new(Ticket::new(bytes::Bytes::new())))
            .await
            .unwrap();

        let batches: Vec<RecordBatch> = FlightRecordBatchStream::new_from_flight_data(
            resp.into_inner().map_err(FlightError::Tonic),
        )
        .try_collect()
        .await
        .unwrap();

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 1);
        let cat_col = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let sch_col = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(cat_col.value(0), "yata");
        assert_eq!(sch_col.value(0), "public");
    }

    #[tokio::test]
    async fn test_flight_sql_tables() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let svc = YataFlightService::new(uri).await.unwrap();

        let cmd = CommandGetTables {
            catalog: None,
            db_schema_filter_pattern: None,
            table_name_filter_pattern: None,
            table_types: vec![],
            include_schema: false,
        };
        let resp = svc
            .do_get_tables(cmd, Request::new(Ticket::new(bytes::Bytes::new())))
            .await
            .unwrap();

        let batches: Vec<RecordBatch> = FlightRecordBatchStream::new_from_flight_data(
            resp.into_inner().map_err(FlightError::Tonic),
        )
        .try_collect()
        .await
        .unwrap();

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 7);
        let name_col = batch.column(2).as_any().downcast_ref::<StringArray>().unwrap();
        let names: Vec<&str> = (0..name_col.len()).map(|i| name_col.value(i)).collect();
        assert!(names.contains(&"yata_messages"));
        assert!(names.contains(&"yata_events"));
    }

    #[tokio::test]
    async fn test_flight_sql_sql_info() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let svc = YataFlightService::new(uri).await.unwrap();

        let cmd = CommandGetSqlInfo { info: vec![] };
        let resp = svc
            .do_get_sql_info(cmd, Request::new(Ticket::new(bytes::Bytes::new())))
            .await
            .unwrap();

        let batches: Vec<RecordBatch> = FlightRecordBatchStream::new_from_flight_data(
            resp.into_inner().map_err(FlightError::Tonic),
        )
        .try_collect()
        .await
        .unwrap();

        assert_eq!(batches.len(), 1);
        assert!(batches[0].num_rows() >= 5);
    }

    #[tokio::test]
    async fn test_flight_sql_statement_query() {
        let dir = tempfile::tempdir().unwrap();
        let graph_uri = dir.path().to_str().unwrap();

        let store = LanceGraphStore::new(graph_uri).await.unwrap();
        let mut props = IndexMap::new();
        props.insert("name".into(), yata_graph::json_to_cypher(&serde_json::json!("Alice")));
        store
            .write_vertices(&[yata_cypher::NodeRef {
                id: "alice".into(),
                labels: vec!["Person".into()],
                props,
            }])
            .await
            .unwrap();

        let svc = YataFlightService::new_with_graph(graph_uri, graph_uri)
            .await
            .unwrap();

        let cmd = CommandStatementQuery {
            query: "MATCH (n:Person) RETURN n.name AS name".into(),
            transaction_id: None,
        };
        let info_resp = svc
            .get_flight_info_statement(cmd, Request::new(FlightDescriptor::new_cmd(vec![])))
            .await
            .unwrap();
        let info = info_resp.into_inner();
        assert!(!info.endpoint.is_empty());

        let ticket_bytes = &info.endpoint[0].ticket.as_ref().unwrap().ticket;
        let any: Any = Message::decode(&ticket_bytes[..]).unwrap();
        let tsq = TicketStatementQuery::decode(any.value).unwrap();

        let resp = svc
            .do_get_statement(tsq, Request::new(Ticket::new(bytes::Bytes::new())))
            .await
            .unwrap();

        let batches: Vec<RecordBatch> = FlightRecordBatchStream::new_from_flight_data(
            resp.into_inner().map_err(FlightError::Tonic),
        )
        .try_collect()
        .await
        .unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        let col = batches[0].column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(col.value(0), r#""Alice""#);
    }

    #[tokio::test]
    async fn test_flight_sql_table_types() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let svc = YataFlightService::new(uri).await.unwrap();

        let cmd = CommandGetTableTypes {};
        let resp = svc
            .do_get_table_types(cmd, Request::new(Ticket::new(bytes::Bytes::new())))
            .await
            .unwrap();

        let batches: Vec<RecordBatch> = FlightRecordBatchStream::new_from_flight_data(
            resp.into_inner().map_err(FlightError::Tonic),
        )
        .try_collect()
        .await
        .unwrap();

        assert_eq!(batches.len(), 1);
        let col = batches[0].column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(col.value(0), "TABLE");
    }

    #[tokio::test]
    async fn test_flight_sql_primary_keys_empty() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let svc = YataFlightService::new(uri).await.unwrap();

        let cmd = CommandGetPrimaryKeys {
            catalog: None,
            db_schema: None,
            table: "yata_messages".into(),
        };
        let resp = svc
            .do_get_primary_keys(cmd, Request::new(Ticket::new(bytes::Bytes::new())))
            .await
            .unwrap();

        let batches: Vec<RecordBatch> = FlightRecordBatchStream::new_from_flight_data(
            resp.into_inner().map_err(FlightError::Tonic),
        )
        .try_collect()
        .await
        .unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0);
    }
}
