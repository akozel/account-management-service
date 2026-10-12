mod config;
mod startup;

use config::ServiceConfig;
use startup::{error::ServiceError, service::Service};

#[tokio::main]
async fn main() -> Result<(), ServiceError> {
    let config = ServiceConfig::load()?;
    let service = Service::prepare(config).await?;
    service.run().await
}
