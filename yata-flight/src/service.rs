use std::pin::Pin;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
    encode::FlightDataEncoderBuilder,
    error::{FlightError, Result as FlightResult},
    flight_service_server::FlightService,
};
use arrow_schema::Schema;
use futures::{Stream, TryStreamExt};
use tonic::{Request, Response, Status, Streaming};

use crate::catalog::YataTableCatalog;
use crate::codec::ScanTicket;

pub struct YataFlightService {
    catalog: Arc<YataTableCatalog>,
}

impl YataFlightService {
    pub fn new(lance_base_uri: impl Into<String>) -> Self {
        Self {
            catalog: Arc::new(YataTableCatalog::new(lance_base_uri)),
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
        let ticket = ScanTicket::from_bytes(&raw_ticket.ticket)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        tracing::debug!(
            table = %ticket.table,
            filter = ?ticket.filter,
            limit = ?ticket.limit,
            "do_get"
        );

        let stream = Self::execute_scan(self.catalog.clone(), ticket).await?;
        Ok(Response::new(stream))
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
