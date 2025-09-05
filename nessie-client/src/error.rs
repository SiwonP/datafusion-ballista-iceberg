use thiserror::Error;

#[derive(Error, Debug)]
pub enum NessieError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Unexpected response format")]
    InvalidResponse(String),

    #[error("URL parse error: {0}")]
    Url(#[from] url::ParseError),
}

impl From<NessieError> for iceberg::Error {
    fn from(value: NessieError) -> Self {
        iceberg::Error::new(iceberg::ErrorKind::DataInvalid, "test")
    }
}

impl From<iceberg::Error> for NessieError {
    fn from(value: iceberg::Error) -> Self {
        todo!()
    }
}
