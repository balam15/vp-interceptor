use crate::config::DebugPayload;

pub fn log_bytes(cfg: &DebugPayload, conn_id: u64, direction: &str, step: &str, bytes: &[u8]) {
    if !cfg.enabled {
        return;
    }

    let limit = cfg.max_bytes.min(bytes.len());
    let shown = &bytes[..limit];
    tracing::info!(
        target: "payload",
        "payload data conn_id={} direction=\"{}\" step=\"{}\" length={} shown={} truncated={} hex={} ascii={:?}",
        conn_id,
        direction,
        step,
        bytes.len(),
        shown.len(),
        bytes.len() > shown.len(),
        hex(shown),
        String::from_utf8_lossy(shown),
    );
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(char::from_digit((b >> 4) as u32, 16).unwrap());
        out.push(char::from_digit((b & 0x0f) as u32, 16).unwrap());
    }
    out
}
