use anyhow::Result;
use std::{collections::HashMap, sync::Arc};
use tokio::{net::TcpListener, sync::Mutex};

pub mod connection;
pub mod respcmd;
pub mod resptype;
pub mod types;
pub mod work;

use crate::{
    connection::{master_listen, replica_listen},
    types::{parse_args, Bulk, Entry},
    work::connect_to_master,
};

#[tokio::main]
async fn main() -> Result<()> {
    let config = parse_args();

    let listener = TcpListener::bind(format!("127.0.0.1:{}", config.read().await.port)).await?;
    let db = Arc::new(Mutex::new(HashMap::<Bulk, Entry>::new()));

    if let Some((ref host, ref port)) = config.read().await.replica_of {
        let config = config.clone();
        let host = host.clone();
        let port = port.clone();
        tokio::spawn(connect_to_master(host, port, config, db.clone()));
    };

    let is_master = config.read().await.is_master();

    if is_master {
        master_listen(listener, db, config).await;
    }
    // rust_analyzer didn't like `else`
    replica_listen(listener, db, config).await;

    unreachable!()
}
