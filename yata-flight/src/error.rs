use thiserror::Error;

#[derive(Debug, Error)]
pub enum FlightError {
    #[error("table not found: {0}")]
    TableNotFound(String),
    #[error("dataset not found: {0}")]
    DatasetNotFound(String),
    #[error("lance error: {0}")]
    Lance(String),
    #[error("arrow error: {0}")]
    Arrow(String),
    #[error("invalid ticket: {0}")]
    InvalidTicket(String),
    #[error("invalid descriptor: {0}")]
    InvalidDescriptor(String),
}

impl From<FlightError> for tonic::Status {
    fn from(e: FlightError) -> Self {
        match &e {
            FlightError::TableNotFound(_) | FlightError::DatasetNotFound(_) => {
                tonic::Status::not_found(e.to_string())
            }
            FlightError::InvalidTicket(_) | FlightError::InvalidDescriptor(_) => {
                tonic::Status::invalid_argument(e.to_string())
            }
            _ => tonic::Status::internal(e.to_string()),
        }
    }
}
