//! Arrow Flight SQL service implementation for yata.
//!
//! Implements a Flight service that handles SQL queries over Arrow Flight protocol.
//! SQL queries are parsed → executed against MutableCsrStore → streamed as Arrow RecordBatch.

use arrow::array::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket,
};
use futures::stream::BoxStream;
use futures::StreamExt;
use std::sync::Arc;
use tonic::{Request, Response, Status, Streaming};

use yata_store::MutableCsrStore;

use crate::executor;
use crate::sql_plan;

/// yata Flight SQL service.
///
/// Wraps a MutableCsrStore and serves SQL queries over Arrow Flight protocol.
/// SQL is passed as the ticket body in DoGet.
pub struct YataFlightSqlService {
    store: Arc<std::sync::Mutex<MutableCsrStore>>,
}

impl YataFlightSqlService {
    pub fn new(store: Arc<std::sync::Mutex<MutableCsrStore>>) -> Self {
        Self { store }
    }

    /// Create a tonic gRPC server for this service.
    pub fn into_server(self) -> FlightServiceServer<Self> {
        FlightServiceServer::new(self)
    }

    fn execute_sql(&self, sql: &str) -> Result<RecordBatch, Status> {
        let plan = sql_plan::parse_select(sql)
            .map_err(|e| Status::invalid_argument(format!("SQL parse error: {}", e)))?;

        let store = self
            .store
            .lock()
            .map_err(|e| Status::internal(e.to_string()))?;
        executor::execute(&store, &plan)
            .map_err(|e| Status::internal(format!("execution error: {}", e)))
    }
}

#[tonic::async_trait]
impl FlightService for YataFlightSqlService {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        let response = HandshakeResponse {
            protocol_version: 0,
            payload: bytes::Bytes::new(),
        };
        Ok(Response::new(
            futures::stream::once(async { Ok(response) }).boxed(),
        ))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let sql = String::from_utf8(descriptor.cmd.to_vec())
            .map_err(|_| Status::invalid_argument("invalid UTF-8"))?;

        let batch = self.execute_sql(&sql)?;
        let schema = batch.schema();

        let ticket = Ticket::new(sql.into_bytes());
        let endpoint = arrow_flight::FlightEndpoint::new().with_ticket(ticket);

        let info = FlightInfo::new()
            .try_with_schema(schema.as_ref())
            .map_err(|e| Status::internal(e.to_string()))?
            .with_endpoint(endpoint)
            .with_total_records(batch.num_rows() as i64)
            .with_total_bytes(-1);

        Ok(Response::new(info))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<arrow_flight::PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }

    /// DoGet: execute SQL from ticket, return Arrow RecordBatch stream.
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let sql = String::from_utf8(ticket.ticket.to_vec())
            .map_err(|_| Status::invalid_argument("invalid UTF-8 in ticket"))?;

        let batch = self.execute_sql(&sql)?;
        let schema = batch.schema();

        let batches: Vec<RecordBatch> = if batch.num_rows() > 0 {
            vec![batch]
        } else {
            vec![]
        };

        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::iter(batches.into_iter().map(Ok)))
            .map(|r| r.map_err(|e| Status::internal(e.to_string())));

        Ok(Response::new(stream.boxed()))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions"))
    }
}
