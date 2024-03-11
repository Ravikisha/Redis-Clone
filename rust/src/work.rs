use anyhow::Result;
use bytes::Bytes;
use std::{
    ops::AddAssign,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{broadcast::Sender, mpsc::UnboundedSender, RwLock},
    task::JoinSet,
    time,
};

use crate::{
    bulk,
    respcmd::{Conf, ConfGet, RESPCmd},
    resptype::RESPType,
    types::{Bulk, Config, Db, Entry},
};

const REPLICATION_ID: &str = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";

pub async fn handle_command_master(
    cmd: RESPCmd,
    db: Db,
    config: Config,
    tx: &UnboundedSender<Bytes>,
    bx: &Sender<Bytes>,
) -> Result<Option<RESPType>> {
    Ok(Some(match cmd {
        RESPCmd::ReplConf((conf, arg)) => handle_replconf(conf, arg, config, tx, bx).await?,
        RESPCmd::Psync((id, offset)) => handle_psync(id, offset, config).await?.into(),
        RESPCmd::Wait((num_replicas, timeout)) => {
            handle_wait(num_replicas, timeout, config).await.into()
        }
        _ => handle_command(cmd, db, config).await?.into(),
    })
    .flatten())
}

pub async fn handle_command(cmd: RESPCmd, db: Db, config: Config) -> Result<Option<RESPType>> {
    Ok(Some(match cmd {
        RESPCmd::Echo(echo) => RESPType::Bulk(Some(echo)).into(),
        RESPCmd::Ping => RESPType::String(String::from("PONG")).into(),
        RESPCmd::Set((key, val, timeout)) => handle_set(key, val, timeout, db).await.into(),
        RESPCmd::Get(key) => handle_get(key, db).await.into(),
        RESPCmd::Info(topic) => handle_info(topic, config).await.into(),
        RESPCmd::ReplConf((c @ Conf::GetAck, arg)) => {
            handle_replconf_replica(c, arg, config).await?
        }
        RESPCmd::FullResync(_) => todo!(),
        RESPCmd::Config((arg, confget)) => handle_config(arg, confget, config).await.into(),
        _ => unimplemented!(), // shouldn't be needed on a replica
    })
    .flatten())
}

async fn handle_config(_arg: Bulk, confget: ConfGet, config: Config) -> RESPType {
    RESPType::Array(match confget {
        ConfGet::Dir => vec![
            RESPType::Bulk(Some(bulk!("dir"))),
            RESPType::Bulk(config.read().await.dir.as_ref().map(|val| bulk!(val))),
        ],
        ConfGet::DbFilename => vec![
            RESPType::Bulk(Some(bulk!("dbfilename"))),
            RESPType::Bulk(
                config
                    .read()
                    .await
                    .dbfilename
                    .as_ref()
                    .map(|val| bulk!(val)),
            ),
        ],
    })
}

async fn handle_wait(min_replicas: usize, timeout: usize, config: Config) -> RESPType {
    async fn ping_replicas(config: Config, num_responses: Arc<RwLock<usize>>, min_replicas: usize) {
        let master_offset = config.read().await.offset;

        let already_acked = config
            .read()
            .await
            .replicas
            .iter()
            .filter(|(_, _, offset)| offset >= &master_offset)
            .count();
        num_responses.write().await.add_assign(already_acked);

        if already_acked >= min_replicas {
            return;
        }

        let mut set = JoinSet::new();
        for (replica, receiver, offset) in config.read().await.replicas.iter() {
            if offset >= &master_offset {
                continue;
            }

            let num_responses = num_responses.clone();
            let replica = replica.clone();
            let mut receiver = receiver.resubscribe();

            set.spawn(async move {
                _ = replica.send(RESPCmd::ReplConf((Conf::GetAck, bulk!("*"))).as_bytes());
                loop {
                    match receiver.recv().await {
                        Ok(mut resp) => match RESPType::parse(&mut resp) {
                            Ok((cmd, _)) => match RESPCmd::parse(cmd) {
                                Ok(RESPCmd::ReplConf((Conf::Ack, _))) => {
                                    num_responses.write().await.add_assign(1);
                                    break;
                                }
                                _ => continue,
                            },
                            Err(_) => continue,
                        },
                        Err(_) => continue,
                    }
                }
            });
        }

        loop {
            set.join_next().await;
            if num_responses.read().await.clone() >= min_replicas {
                set.abort_all()
            }
        }
    }

    let num_responses = Arc::new(RwLock::const_new(0));
    if timeout == 0 {
        ping_replicas(config, num_responses.clone(), min_replicas).await;
    } else {
        let sleep = time::sleep(Duration::from_millis(timeout as u64));

        tokio::select! {
            _ = ping_replicas(config, num_responses.clone(), min_replicas) => {}
            _ = sleep => {}
        };
    }

    let resp = num_responses.read().await.clone();
    RESPType::Integer(resp as isize)
}

async fn handle_replconf(
    conf: Conf,
    arg: Bulk,
    config: Config,
    tx: &UnboundedSender<Bytes>,
    bx: &Sender<Bytes>,
) -> Result<Option<RESPType>> {
    match conf {
        Conf::ListeningPort => {
            config
                .write()
                .await
                .replicas
                .push((tx.clone(), bx.subscribe(), 0));
            Ok(RESPType::String(String::from("OK")).into())
        }
        other => handle_replconf_replica(other, arg, config).await,
    }
}

async fn handle_replconf_replica(
    conf: Conf,
    _arg: Bulk,
    config: Config,
) -> Result<Option<RESPType>> {
    Ok(match conf {
        Conf::Capa => RESPType::String(String::from("OK")).into(),
        Conf::GetAck => RESPCmd::ReplConf((
            Conf::Ack,
            bulk!(config.read().await.offset.to_string().as_str()),
        ))
        .to_command()
        .into(),
        Conf::Ack => None, // TODO: update offset
        Conf::ListeningPort => unreachable!(),
    })
}

async fn handle_set(key: Bulk, val: Bulk, timeout: Option<SystemTime>, db: Db) -> RESPType {
    let mut db = db.lock().await;
    db.insert(key, Entry { val, timeout });

    RESPType::String(String::from("OK"))
}

async fn handle_get(key: Bulk, db: Db) -> RESPType {
    let db = db.lock().await;

    let val = db.get(&key).and_then(|e| {
        if e.timeout.is_some_and(|timeout| timeout < SystemTime::now()) {
            None
        } else {
            Some(e.val.clone())
        }
    });

    RESPType::Bulk(val)
}

async fn handle_info(topic: Option<Bulk>, config: Config) -> RESPType {
    // TODO: handle INFO with no topic (all sections)
    let Some(topic) = topic else { unimplemented!() };

    RESPType::Bulk(Some(match topic.as_bytes() {
        b"replication" => info_replication(config).await,
        _ => unimplemented!(),
    }))
}

async fn info_replication(config: Config) -> Bulk {
    let role = match config.read().await.is_master() {
        true => "master",
        false => "slave",
    };

    // TODO: fetch master offset if this is a replica
    Bulk::from(
        format!(
            "\
role:{role}
master_replid:{REPLICATION_ID}
master_repl_offset:{}
",
            0 // TODO: should this be the current offset?
        )
        .as_str(),
    )
}

async fn handle_psync(_id: Bulk, _offset: Bulk, config: Config) -> Result<RESPType> {
    let mut rdb = Vec::new();
    File::open("./redis.rdb")
        .await?
        .read_to_end(&mut rdb)
        .await?;

    config.write().await.offset = 0;

    Ok(RESPType::Multi(vec![
        RESPCmd::FullResync((bulk!(REPLICATION_ID), bulk!("0"))).to_command(),
        RESPType::RDBFile(rdb),
    ]))
}

pub async fn connect_to_master(host: String, port: String, config: Config, db: Db) -> Result<()> {
    let mut buf = [0; 1024]; // TODO: can we read straight into Bytes
    let mut conn = TcpStream::connect(format!("{host}:{port}")).await?;

    conn.write_all(&RESPCmd::Ping.as_bytes()).await?;

    while let 0 = conn.read(&mut buf).await? {} // TODO: check OK

    conn.write_all(
        &RESPCmd::ReplConf((Conf::ListeningPort, Bulk::from(&config.read().await.port))).as_bytes(),
    )
    .await?;

    while let 0 = conn.read(&mut buf).await? {} // TODO: check OK

    conn.write_all(&RESPCmd::ReplConf((Conf::Capa, Bulk::from("psync2"))).as_bytes())
        .await?;

    while let 0 = conn.read(&mut buf).await? {} // TODO: check OK

    conn.write_all(&RESPCmd::Psync((Bulk::from("?"), Bulk::from("-1"))).as_bytes())
        .await?;

    while let 0 = conn.read(&mut buf).await? {}

    let mut buff = Bytes::copy_from_slice(&buf);
    let _full_resync = RESPType::parse(&mut buff)?;

    // Recieve RDBFile
    match RESPType::parse(&mut buff) {
        Ok(_) => {}
        Err(_) => {
            while let 0 = conn.read(&mut buf).await? {}
            let mut buff = Bytes::copy_from_slice(&buf);
            RESPType::parse(&mut buff)?;
        }
    }

    // TODO: verify full resync and rdb file

    // TODO: reduce hacky duplication
    while let Ok((cmd, len)) = RESPType::parse(&mut buff) {
        let cmd = RESPCmd::parse(cmd)?;
        if let Some(resp) = handle_command(cmd.clone(), db.clone(), config.clone()).await? {
            match cmd {
                RESPCmd::ReplConf((Conf::GetAck, _)) => conn.write_all(&resp.as_bytes()).await?,
                _ => {}
            }
        }

        config.write().await.offset += len;
    }

    loop {
        let len = conn.read(&mut buf).await?;
        if len == 0 {
            continue;
        }

        let mut buf = Bytes::copy_from_slice(&buf[..len]);
        while let Ok((cmd, len)) = RESPType::parse(&mut buf) {
            let cmd = RESPCmd::parse(cmd)?;
            if let Some(resp) = handle_command(cmd.clone(), db.clone(), config.clone()).await? {
                match cmd {
                    RESPCmd::ReplConf((Conf::GetAck, _)) => {
                        conn.write_all(&resp.as_bytes()).await?
                    }
                    _ => {}
                }
            }

            config.write().await.offset += len;
        }
    }
}
