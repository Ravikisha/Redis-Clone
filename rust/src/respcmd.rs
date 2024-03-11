use anyhow::{bail, Result};
use bytes::Bytes;
use std::{
    ops::Add,
    time::{Duration, SystemTime},
};

use crate::{bulk, resptype::RESPType, types::Bulk};

#[derive(Clone, Debug)]
pub enum Conf {
    ListeningPort,
    Capa,
    GetAck,
    Ack,
}

#[derive(Clone, Debug)]
pub enum ConfGet {
    Dir,
    DbFilename,
}

#[derive(Clone, Debug)]
pub enum RESPCmd {
    Echo(Bulk),
    Ping,
    Get(Bulk),
    Set((Bulk, Bulk, Option<SystemTime>)),
    Info(Option<Bulk>),
    ReplConf((Conf, Bulk)),
    Psync((Bulk, Bulk)),
    FullResync((Bulk, Bulk)),
    Wait((usize, usize)),
    Config((Bulk, ConfGet)),
}

macro_rules! respcmd {
    ($($word:expr) +) => {
        RESPType::Array(vec![
            $(RESPType::Bulk(Some($word)),)+
        ])
    };
}

impl RESPCmd {
    pub fn to_command(self) -> RESPType {
        match self {
            RESPCmd::Echo(_) => todo!(),
            RESPCmd::Ping => respcmd!(bulk!("PING")),
            RESPCmd::Get(_) => todo!(),
            RESPCmd::Set((key, val, expiry)) => Self::handle_set(key, val, expiry),
            RESPCmd::Info(_) => todo!(),
            RESPCmd::ReplConf((conf, bulk)) => Self::handle_replconf(conf, bulk),
            RESPCmd::Psync((id, offset)) => respcmd!(bulk!("PSYNC") id offset),
            RESPCmd::FullResync((id, offset)) => Self::handle_full_resync(id, offset),
            RESPCmd::Wait(_) => todo!(),
            RESPCmd::Config(_) => todo!(),
        }
    }

    pub fn as_bytes(self) -> Bytes {
        self.to_command().as_bytes().freeze()
    }

    fn handle_replconf(conf: Conf, bulk: Bulk) -> RESPType {
        respcmd!(bulk!("REPLCONF") bulk!(match conf {
            Conf::ListeningPort => "listening-port",
            Conf::Capa => "capa",
            Conf::GetAck => "GETACK",
            Conf::Ack => "ACK",
        }) bulk)
    }

    fn handle_full_resync(id: Bulk, offset: Bulk) -> RESPType {
        RESPType::String(format!("FULLRESYNC {id} {offset}"))
    }

    fn handle_set(key: Bulk, val: Bulk, expiry: Option<SystemTime>) -> RESPType {
        let mut array = vec![
            RESPType::Bulk(Some(bulk!("SET"))),
            RESPType::Bulk(Some(key)),
            RESPType::Bulk(Some(val)),
        ];

        if let Some(expiry) = expiry {
            let Ok(timeout) = expiry.duration_since(SystemTime::now()) else {
                // key has expired, do nothing
                // TODO: actually do nothing
                return respcmd!(bulk!("PING"));
            };
            array.push(RESPType::Bulk(Some(bulk!("PX"))));
            array.push(RESPType::Bulk(Some(bulk!(timeout
                .as_millis()
                .to_string()
                .as_str()))));
        };

        RESPType::Array(array)
    }
}

impl RESPCmd {
    pub fn parse(cmd: RESPType) -> Result<Self> {
        let RESPType::Array(cmd) = cmd else {
            bail!("Top level command must be array");
        };

        let mut parts = cmd.into_iter();
        let Some(RESPType::Bulk(Some(cmd))) = parts.next() else {
            bail!("Command must be non-null bulk string");
        };

        Ok(match cmd.data.to_ascii_uppercase().as_slice() {
            b"PING" => Self::Ping,
            b"ECHO" => Self::Echo(Self::parse_echo(parts)?),
            b"SET" => Self::Set(Self::parse_set(parts)?),
            b"GET" => Self::Get(Self::parse_get(parts)?),
            b"INFO" => Self::Info(Self::parse_info(parts)?),
            b"REPLCONF" => Self::ReplConf(Self::parse_replconf(parts)?),
            b"PSYNC" => Self::Psync(Self::parse_psync(parts)?),
            b"WAIT" => Self::Wait(Self::parse_wait(parts)?),
            b"CONFIG" => Self::Config(Self::parse_config(parts)?),
            // TODO: FULLRESYNC being handled seperately due to being simple string
            _ => Self::Ping, // try not to crash
        })
    }

    fn parse_config(mut parts: impl Iterator<Item = RESPType>) -> Result<(Bulk, ConfGet)> {
        let Some(RESPType::Bulk(Some(arg))) = parts.next() else {
            bail!("Config requires an argument");
        };

        let Some(RESPType::Bulk(Some(section))) = parts.next() else {
            bail!("Config requires an argument");
        };

        let section = match section.data.to_ascii_lowercase().as_slice() {
            b"dir" => ConfGet::Dir,
            b"dbfilename" => ConfGet::DbFilename,
            _ => bail!("Invalid Config argument"),
        };

        Ok((arg, section))
    }

    fn parse_echo(mut parts: impl Iterator<Item = RESPType>) -> Result<Bulk> {
        let Some(RESPType::Bulk(Some(echo))) = parts.next() else {
            bail!("Echo must take a non-null bulk string");
        };

        Ok(echo)
    }

    fn parse_set(
        mut parts: impl Iterator<Item = RESPType>,
    ) -> Result<(Bulk, Bulk, Option<SystemTime>)> {
        let Some(RESPType::Bulk(Some(key))) = parts.next() else {
            bail!("Set requires a key");
        };

        let Some(RESPType::Bulk(Some(val))) = parts.next() else {
            bail!("Set requires a value");
        };

        let mut timeout = None;
        if let Some(RESPType::Bulk(Some(bulk))) = parts.next() {
            if bulk.data.as_ref().to_ascii_uppercase() == b"PX" {
                let Some(RESPType::Bulk(Some(mut i))) = parts.next() else {
                    bail!("PX requires an expiry");
                };

                let i = RESPType::parse_uinteger(&mut i.data)?;
                timeout = Some(SystemTime::now().add(Duration::from_millis(i as u64)))
            }
        }

        Ok((key, val, timeout))
    }

    fn parse_get(mut parts: impl Iterator<Item = RESPType>) -> Result<Bulk> {
        let Some(RESPType::Bulk(Some(key))) = parts.next() else {
            bail!("Set requires a key");
        };

        Ok(key)
    }

    fn parse_info(mut parts: impl Iterator<Item = RESPType>) -> Result<Option<Bulk>> {
        Ok(if let Some(RESPType::Bulk(Some(topic))) = parts.next() {
            Some(topic)
        } else {
            None
        })
    }

    fn parse_replconf(mut parts: impl Iterator<Item = RESPType>) -> Result<(Conf, Bulk)> {
        let Some(RESPType::Bulk(Some(conf))) = parts.next() else {
            bail!("Missing replconf arguments");
        };

        let Some(RESPType::Bulk(Some(state))) = parts.next() else {
            bail!("Missing replconf arguments");
        };

        let conf = match conf.data.to_ascii_lowercase().as_slice() {
            b"listening-port" => Conf::ListeningPort,
            b"capa" => Conf::Capa,
            b"getack" => Conf::GetAck,
            b"ack" => Conf::Ack,
            _ => bail!("Invalid replconf argument"),
        };

        Ok((conf, state))
    }

    fn parse_psync(mut parts: impl Iterator<Item = RESPType>) -> Result<(Bulk, Bulk)> {
        let Some(RESPType::Bulk(Some(repl_id))) = parts.next() else {
            bail!("Missing repl_id");
        };

        let Some(RESPType::Bulk(Some(offset))) = parts.next() else {
            bail!("Missing offset");
        };

        Ok((repl_id, offset))
    }

    fn parse_wait(mut parts: impl Iterator<Item = RESPType>) -> Result<(usize, usize)> {
        let Some(RESPType::Bulk(Some(mut num_replicas))) = parts.next() else {
            bail!("Wait requires num_replicas");
        };

        let Some(RESPType::Bulk(Some(mut timeout))) = parts.next() else {
            bail!("Wait requires timeout");
        };

        let num_replicas = RESPType::parse_uinteger(&mut num_replicas.data)?;
        let timeout = RESPType::parse_uinteger(&mut timeout.data)?;

        Ok((num_replicas, timeout))
    }
}
