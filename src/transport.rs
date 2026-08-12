use std::fs;
use std::io::{Read, Write};
use std::path::Path;

use anyhow::{Context, Result, bail};

use crate::protocol::{AgentMessage, ClientMessage, Plan, read_json, write_json};
use crate::snapshot::{Generation, validate_relative_path};

/// Runs one plan over a connected agent transport. The reader and writer may be
/// a local child process or an SSH child process.
pub fn transact<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut W,
    local_root: &Path,
    plan: Plan,
) -> Result<Generation> {
    write_json(writer, &ClientMessage::Plan(plan))?;
    let requested = match read_json(reader)? {
        Some(AgentMessage::NeedPayloads { paths }) => paths,
        Some(AgentMessage::Error { message }) => bail!("agent rejected plan: {message}"),
        Some(message) => bail!("unexpected agent response: {message:?}"),
        None => bail!("agent closed before requesting payloads"),
    };
    for path in requested {
        validate_relative_path(&path)?;
        let source = local_root.join(&path);
        let metadata = fs::symlink_metadata(&source)
            .with_context(|| format!("read payload metadata for {}", path.display()))?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            bail!("agent requested a non-file payload: {}", path.display());
        }
        write_json(
            writer,
            &ClientMessage::Payload {
                path: path.clone(),
                length: metadata.len(),
            },
        )?;
        let mut file = fs::File::open(&source)?;
        std::io::copy(&mut file, writer)?;
        writer.flush()?;
    }
    match read_json(reader)? {
        Some(AgentMessage::Ack { generation }) => Ok(generation),
        Some(AgentMessage::Error { message }) => bail!("agent failed to apply plan: {message}"),
        Some(message) => bail!("unexpected agent response: {message:?}"),
        None => bail!("agent closed before acknowledging plan"),
    }
}
