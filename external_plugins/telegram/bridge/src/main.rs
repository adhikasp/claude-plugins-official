//! tg-bridge: a ~1 MB replacement for the per-session `bun server.ts` Telegram client.
//!
//! Claude Code speaks MCP (newline-delimited JSON-RPC) on our stdin/stdout. We relay it to the
//! router over a unix socket; the router hosts the actual MCP server for this route.
//!
//! Resilience: if the router restarts, we reconnect and replay the session's `initialize` request
//! and `notifications/initialized`, swallowing the duplicate initialize response, so Claude never
//! notices. Requests that were in flight during the outage get a JSON-RPC error back.
//!
//! Env: TELEGRAM_ROUTE (required), TELEGRAM_ROUTE_ADMIN=1, TELEGRAM_ROUTER_SOCK, TELEGRAM_STATE_DIR.

use std::collections::HashSet;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

/// Top-level `id` of a JSON-RPC message as raw JSON text (number or string), if any.
fn json_id(line: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(line).ok()?;
    match v.get("id")? {
        serde_json::Value::Null => None,
        id => Some(id.to_string()),
    }
}

fn method_of(line: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(line).ok()?;
    v.get("method")?.as_str().map(|s| s.to_string())
}

fn is_request(line: &str) -> bool {
    method_of(line).is_some() && json_id(line).is_some()
}

fn state_dir() -> PathBuf {
    if let Ok(d) = std::env::var("TELEGRAM_STATE_DIR") {
        return PathBuf::from(d);
    }
    let base = std::env::var("CLAUDE_CONFIG_DIR").map(PathBuf::from).unwrap_or_else(|_| {
        PathBuf::from(std::env::var("HOME").unwrap_or_else(|_| ".".into())).join(".claude")
    });
    base.join("channels").join("telegram")
}

struct Shared {
    /// the session's `initialize` request and `notifications/initialized`, for replay after reconnect
    init_req: Option<String>,
    init_id: Option<String>,
    initialized: Option<String>,
    /// ids of requests sent to the router that have no response yet
    pending: HashSet<String>,
    /// when set, swallow the response with this id (it answers a replayed initialize)
    swallow: Option<String>,
}

fn write_stdout(line: &str) {
    let out = std::io::stdout();
    let mut o = out.lock();
    let _ = o.write_all(line.as_bytes());
    let _ = o.write_all(b"\n");
    let _ = o.flush();
}

fn main() {
    let route = match std::env::var("TELEGRAM_ROUTE") {
        Ok(r) if !r.is_empty() => r,
        // Not a Telegram session (interactive pane, headless job, subagent). Exiting would make Claude cache the
        // server as failed and disable it for every later session, so answer the handshake with zero tools instead.
        _ => return idle_server(),
    };
    let admin = std::env::var("TELEGRAM_ROUTE_ADMIN").map(|v| v == "1").unwrap_or(false);
    let sock_path = std::env::var("TELEGRAM_ROUTER_SOCK")
        .map(PathBuf::from)
        .unwrap_or_else(|_| state_dir().join("router.sock"));

    let shared = Arc::new(Mutex::new(Shared {
        init_req: None,
        init_id: None,
        initialized: None,
        pending: HashSet::new(),
        swallow: None,
    }));

    // Single writer channel to whichever socket is currently live.
    let (to_router_tx, to_router_rx): (Sender<String>, Receiver<String>) = channel();

    // stdin reader: Claude -> router. Exits the process on EOF (Claude closed the session).
    {
        let shared = shared.clone();
        let tx = to_router_tx.clone();
        thread::spawn(move || {
            let stdin = std::io::stdin();
            for line in BufReader::new(stdin.lock()).lines() {
                let Ok(line) = line else { break };
                if line.trim().is_empty() {
                    continue;
                }
                {
                    let mut sh = shared.lock().unwrap();
                    let method = method_of(&line);
                    if method.as_deref() == Some("initialize") {
                        sh.init_id = json_id(&line);
                        sh.init_req = Some(line.clone());
                    } else if method.as_deref() == Some("notifications/initialized") {
                        sh.initialized = Some(line.clone());
                    }
                    if is_request(&line) {
                        if let Some(id) = json_id(&line) {
                            sh.pending.insert(id);
                        }
                    }
                }
                if tx.send(line).is_err() {
                    break;
                }
            }
            std::process::exit(0);
        });
    }

    let mut first = true;
    loop {
        let stream = match UnixStream::connect(&sock_path) {
            Ok(s) => s,
            Err(_) => {
                thread::sleep(Duration::from_millis(1000));
                continue;
            }
        };
        let hello = format!(
            "{{\"type\":\"bridge\",\"route\":{},\"admin\":{}}}\n",
            json_quote(&route),
            admin
        );
        let mut w = match stream.try_clone() {
            Ok(w) => w,
            Err(_) => continue,
        };
        if w.write_all(hello.as_bytes()).is_err() {
            thread::sleep(Duration::from_millis(1000));
            continue;
        }

        if !first {
            // Replay the handshake so the fresh router-side MCP server is in the same state.
            let (init, id, inited) = {
                let mut sh = shared.lock().unwrap();
                // fail whatever was in flight when the old connection dropped
                let dead: Vec<String> = sh.pending.drain().collect();
                for id in dead {
                    if sh.init_id.as_deref() == Some(id.as_str()) {
                        continue;
                    }
                    write_stdout(&format!(
                        "{{\"jsonrpc\":\"2.0\",\"id\":{},\"error\":{{\"code\":-32000,\"message\":\"telegram router restarted; retry\"}}}}",
                        id
                    ));
                }
                if sh.init_req.is_some() {
                    sh.swallow = sh.init_id.clone();
                }
                (sh.init_req.clone(), sh.init_id.clone(), sh.initialized.clone())
            };
            let _ = id;
            if let Some(i) = init {
                let _ = w.write_all(format!("{}\n", i).as_bytes());
            }
            if let Some(i) = inited {
                let _ = w.write_all(format!("{}\n", i).as_bytes());
            }
        }
        first = false;

        // router -> stdout reader
        let reader_shared = shared.clone();
        let rstream = match stream.try_clone() {
            Ok(r) => r,
            Err(_) => continue,
        };
        let reader = thread::spawn(move || {
            for line in BufReader::new(rstream).lines() {
                let Ok(line) = line else { break };
                if line.trim().is_empty() {
                    continue;
                }
                // a response has an id and no method
                if method_of(&line).is_none() {
                    if let Some(id) = json_id(&line) {
                        let mut sh = reader_shared.lock().unwrap();
                        if sh.swallow.as_deref() == Some(id.as_str()) {
                            sh.swallow = None;
                            continue;
                        }
                        sh.pending.remove(&id);
                    }
                }
                write_stdout(&line);
            }
        });

        // writer: drain the channel into the socket until it breaks
        loop {
            match to_router_rx.recv_timeout(Duration::from_millis(500)) {
                Ok(line) => {
                    if w.write_all(line.as_bytes()).is_err() || w.write_all(b"\n").is_err() {
                        // lost this message; its pending id gets failed on reconnect
                        break;
                    }
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    if reader.is_finished() {
                        break;
                    }
                }
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return,
            }
            if reader.is_finished() {
                break;
            }
        }
        let _ = stream.shutdown(std::net::Shutdown::Both);
        let _ = reader.join();
        thread::sleep(Duration::from_millis(500));
    }
}

fn json_quote(s: &str) -> String {
    serde_json::Value::String(s.to_string()).to_string()
}

/// MCP server with no tools, for claude processes that load the plugin but serve no Telegram route.
fn idle_server() {
    let stdin = std::io::stdin();
    for line in BufReader::new(stdin.lock()).lines() {
        let Ok(line) = line else { break };
        let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) else { continue };
        let (Some(method), Some(id)) = (v.get("method").and_then(|m| m.as_str()), v.get("id")) else { continue };
        let result = match method {
            "initialize" => serde_json::json!({
                "protocolVersion": v["params"]["protocolVersion"].clone(),
                "capabilities": { "tools": {} },
                "serverInfo": { "name": "telegram", "version": "0.0.0" }
            }),
            "tools/list" => serde_json::json!({ "tools": [] }),
            "ping" => serde_json::json!({}),
            _ => {
                write_stdout(&serde_json::json!({"jsonrpc":"2.0","id":id,"error":{"code":-32601,"message":"method not found"}}).to_string());
                continue;
            }
        };
        write_stdout(&serde_json::json!({"jsonrpc":"2.0","id":id,"result":result}).to_string());
    }
}
