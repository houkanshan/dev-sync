use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::snapshot::{Entry, Generation};

const MAX_JSON_FRAME: usize = 64 * 1024 * 1024;
pub const PROTOCOL_VERSION: u32 = 2;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum PlanKind {
    Full {
        entries: BTreeMap<PathBuf, Entry>,
    },
    Delta {
        changes: BTreeMap<PathBuf, Option<Entry>>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Plan {
    pub expected_generation: Generation,
    pub expected_state_id: String,
    pub generation: Generation,
    pub state_id: String,
    #[serde(flatten)]
    pub kind: PlanKind,
}

impl Plan {
    pub fn entry(&self, path: &PathBuf) -> Option<&Entry> {
        match &self.kind {
            PlanKind::Full { entries } => entries.get(path),
            PlanKind::Delta { changes } => changes.get(path).and_then(Option::as_ref),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMessage {
    Hello { version: u32 },
    Plan(Plan),
    Payload { path: PathBuf, length: u64 },
    Done,
    Complete,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AgentMessage {
    Hello {
        version: u32,
        generation: Generation,
        state_id: String,
        recovery_required: bool,
    },
    NeedPayloads {
        paths: Vec<PathBuf>,
    },
    Ack {
        generation: Generation,
        state_id: String,
    },
    Error {
        message: String,
    },
}

pub fn write_json<W: Write, T: Serialize>(writer: &mut W, value: &T) -> Result<()> {
    let bytes = serde_json::to_vec(value)?;
    let length = u32::try_from(bytes.len()).context("JSON protocol frame is too large")?;
    writer.write_all(&length.to_be_bytes())?;
    writer.write_all(&bytes)?;
    writer.flush()?;
    Ok(())
}

pub fn read_json<R: Read, T: DeserializeOwned>(reader: &mut R) -> Result<Option<T>> {
    let mut prefix = [0; 4];
    match reader.read_exact(&mut prefix) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    let length = u32::from_be_bytes(prefix) as usize;
    if length > MAX_JSON_FRAME {
        bail!("JSON protocol frame exceeds {MAX_JSON_FRAME} bytes");
    }
    let mut bytes = vec![0; length];
    reader.read_exact(&mut bytes)?;
    Ok(Some(serde_json::from_slice(&bytes)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn framed_json_round_trips() {
        let message = AgentMessage::NeedPayloads {
            paths: vec![PathBuf::from("src/main.rs")],
        };
        let mut bytes = Vec::new();
        write_json(&mut bytes, &message).unwrap();
        assert_eq!(read_json(&mut bytes.as_slice()).unwrap(), Some(message));
    }
}
