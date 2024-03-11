use anyhow::{bail, Result};
use bytes::Bytes;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{tcp::OwnedReadHalf, TcpListener, TcpStream},
    sync::{
        broadcast::{self, error::RecvError},
        mpsc::{self, UnboundedSender},
    },
};

use crate::{
    respcmd::RESPCmd,
    resptype::RESPType,
    types::{Config, Db},
    work,
};

pub async fn master_listen(listener: TcpListener, db: Db, config: Config) -> ! {
    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let (tx, mut rx) = mpsc::unbounded_channel::<Bytes>();
                let (reader, mut sender) = stream.into_split();
                tokio::spawn(async move {
                    while let Some(i) = rx.recv().await {
                        sender.write_all(&i).await.expect("problem sending")
                    }
                });

                let db = db.clone();
                let config = config.clone();
                tokio::spawn(async move {
                    master_handle_connection(tx, reader, db, config)
                        .await
                        .map_err(|e| println!("error: {}", e))
                });
            }
            Err(e) => println!("error: {}", e),
        }
    }
}

pub async fn replica_listen(listener: TcpListener, db: Db, config: Config) -> ! {
    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let db = db.clone();
                let config = config.clone();
                tokio::spawn(async move {
                    replica_handle_connection(stream, db, config)
                        .await
                        .map_err(|e| println!("error: {}", e))
                });
            }
            Err(e) => println!("error: {}", e),
        }
    }
}

pub async fn master_handle_connection(
    tx: UnboundedSender<Bytes>,
    mut reader: OwnedReadHalf,
    db: Db,
    config: Config,
) -> Result<()> {
    let (bx, mut rx) = broadcast::channel(16);
    let bx2 = bx.clone();

    tokio::spawn(async move {
        let mut buf = [0; 1024];
        loop {
            let len = reader.read(&mut buf).await.expect("problem reading");
            if len == 0 {
                continue;
            }

            let buf = Bytes::copy_from_slice(&buf[..len]);
            bx.send(buf).expect("problem sending");
        }
    });

    loop {
        let mut buf = match rx.recv().await {
            Ok(b) => b,
            Err(RecvError::Lagged(n)) => bail!("missed {n} commands"),
            e @ Err(_) => e?,
        };

        while let Ok((cmd, len)) = RESPType::parse(&mut buf) {
            let cmd = RESPCmd::parse(cmd)?;
            match cmd {
                RESPCmd::Set(_) => {
                    for (replica, _, _) in config.write().await.replicas.iter_mut() {
                        replica.send(cmd.clone().as_bytes())?;
                    }
                }
                _ => {}
            }

            if let Some(resp) =
                work::handle_command_master(cmd.clone(), db.clone(), config.clone(), &tx, &bx2)
                    .await?
            {
                tx.send(resp.as_bytes().freeze())?;
            };

            match cmd {
                RESPCmd::Set(_) => {
                    config.write().await.offset += len;
                }
                _ => {}
            }
        }
    }
}

pub async fn replica_handle_connection(
    mut stream: TcpStream,
    db: Db,
    config: Config,
) -> Result<()> {
    let mut buf = [0; 1024]; // TODO: can we read straight into Bytes

    loop {
        let len = stream.read(&mut buf).await?;
        if len == 0 {
            continue;
        }

        let mut buf = Bytes::copy_from_slice(&buf[..len]);
        while let Ok((cmd, len)) = RESPType::parse(&mut buf) {
            let cmd = RESPCmd::parse(cmd)?;
            if let Some(resp) = work::handle_command(cmd, db.clone(), config.clone()).await? {
                stream.write_all(&resp.as_bytes()).await?;
            }

            config.write().await.offset += len;
        }
    }
}
