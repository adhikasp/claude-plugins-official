//! tg-router: Rust port of the Telegram router (TELEGRAM_ROLE=router in server.ts).
//!
//! One long-polling Bot API consumer; one Claude session per route. Sessions attach through `tg-bridge`
//! (the router hosts each session's MCP server) or, for the old fallback, as legacy bun clients ("hello").
mod access;
mod md;
mod mcp;
mod tg;

use access::Access;
use fancy_regex::Regex;
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

pub fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

pub struct Cfg {
    pub token: String,
    pub state_dir: PathBuf,
    pub access_file: PathBuf,
    pub inbox_dir: PathBuf,
    pub sock: PathBuf,
    pub topics_file: PathBuf,
    pub routes_file: PathBuf,
    pub is_static: bool,
    pub tg_dir: PathBuf,
}

#[derive(Clone)]
pub struct Client {
    pub conn: u64,
    pub tx: UnboundedSender<String>,
    pub bridge: bool,
}

#[derive(Default)]
pub struct State {
    pub clients: HashMap<String, Client>,
    pub queues: HashMap<String, Vec<Value>>,
    pub spawning: HashMap<String, tokio::task::AbortHandle>,
    pub booting: HashSet<String>,
    pub perm_owners: HashMap<String, String>,
    pub pending_ack: HashMap<String, Vec<(String, i64)>>,
    pub pending_perms: HashMap<String, (String, String, String)>,
    pub topics: Map<String, Value>,
    pub registry: Map<String, Value>,
    pub next_conn: u64,
}

pub struct App {
    pub tg: tg::Tg,
    pub cfg: Cfg,
    pub boot_access: Option<Access>,
    pub bot_username: Mutex<String>,
    pub st: Mutex<State>,
}

impl App {
    pub fn access(&self) -> Access {
        self.boot_access.clone().unwrap_or_else(|| Access::load(&self.cfg.access_file))
    }
    pub fn assert_allowed_chat(&self, chat: &str) -> Result<(), String> {
        let a = self.access();
        if a.allow_from.iter().any(|x| x == chat) || a.groups.contains_key(chat) {
            Ok(())
        } else {
            Err(format!("chat {chat} is not allowlisted — add via /telegram:access"))
        }
    }
}

fn re(cell: &'static OnceLock<Regex>, p: &str) -> &'static Regex {
    cell.get_or_init(|| Regex::new(p).unwrap())
}

fn safe_name(s: &str) -> String {
    s.chars().map(|c| if "<>[]\r\n;".contains(c) { '_' } else { c }).collect()
}

fn iso(secs: i64) -> String {
    // civil-from-days (Howard Hinnant)
    let days = secs.div_euclid(86400);
    let rem = secs.rem_euclid(86400);
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z.rem_euclid(146097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.000Z", y, m, d, rem / 3600, (rem % 3600) / 60, rem % 60)
}

fn save_json(file: &PathBuf, v: &Map<String, Value>) {
    use std::os::unix::fs::PermissionsExt;
    if let Ok(s) = serde_json::to_string_pretty(&Value::Object(v.clone())) {
        if std::fs::write(file, s + "\n").is_ok() {
            let _ = std::fs::set_permissions(file, std::fs::Permissions::from_mode(0o600));
        }
    }
}

// ------------------------------------------------------------------ routing

#[derive(Clone)]
pub struct RouteInfo {
    pub route: String,
    pub chat: String,
    pub thread: i64,
    pub chat_title: Option<String>,
    pub topic_title: Option<String>,
}

impl App {
    fn learn_topic(&self, chat: &str, thread: i64, name: Option<&str>) {
        let Some(name) = name else { return };
        if thread == 0 {
            return;
        }
        let mut st = self.st.lock().unwrap();
        let t = thread.to_string();
        if st.topics.get(chat).and_then(|c| c.get(&t)).and_then(|v| v.as_str()) == Some(name) {
            return;
        }
        let entry = st.topics.entry(chat.to_string()).or_insert_with(|| json!({}));
        entry[&t] = json!(name);
        save_json(&self.cfg.topics_file, &st.topics);
    }

    fn route_for(&self, msg: &Value) -> RouteInfo {
        let chat = msg["chat"]["id"].to_string();
        let thread = if msg["is_topic_message"].as_bool() == Some(true) { msg["message_thread_id"].as_i64().unwrap_or(0) } else { 0 };
        self.learn_topic(&chat, thread, msg["reply_to_message"]["forum_topic_created"]["name"].as_str());
        let topic_title = |app: &App| -> Option<String> {
            if thread == 0 {
                return None;
            }
            app.st.lock().unwrap().topics.get(&chat).and_then(|c| c.get(thread.to_string())).and_then(|v| v.as_str()).map(|s| s.to_string())
        };
        if msg["chat"]["type"].as_str() == Some("private") {
            let uid = msg["from"]["id"].to_string();
            return RouteInfo {
                route: if thread != 0 { format!("dm:{uid}:{thread}") } else { format!("dm:{uid}") },
                chat: chat.clone(),
                thread,
                chat_title: None,
                topic_title: topic_title(self),
            };
        }
        let title = msg["chat"]["title"].as_str().map(|s| s.to_string());
        let tt = if thread != 0 {
            topic_title(self)
        } else if msg["chat"]["is_forum"].as_bool() == Some(true) {
            Some("General".to_string())
        } else {
            None
        };
        RouteInfo { route: format!("{chat}:{thread}"), chat: chat.clone(), thread, chat_title: title, topic_title: tt }
    }

    fn touch(&self, info: &RouteInfo) {
        let mut st = self.st.lock().unwrap();
        let connected = st.clients.contains_key(&info.route);
        let prev = st.registry.get(&info.route).cloned().unwrap_or_else(|| json!({}));
        let mut e = prev.as_object().cloned().unwrap_or_default();
        e.insert("chat".into(), json!(info.chat));
        e.insert("thread".into(), json!(info.thread));
        if let Some(t) = info.topic_title.clone().or(info.chat_title.clone()).or_else(|| prev["title"].as_str().map(|s| s.to_string())) {
            e.insert("title".into(), json!(t));
        }
        if let Some(t) = info.chat_title.clone().or_else(|| prev["chatTitle"].as_str().map(|s| s.to_string())) {
            e.insert("chatTitle".into(), json!(t));
        }
        e.insert("lastActive".into(), json!(now_ms()));
        e.insert("connected".into(), json!(connected));
        st.registry.insert(info.route.clone(), Value::Object(e));
        save_json(&self.cfg.routes_file, &st.registry);
    }

    fn set_connected(&self, route: &str, on: bool) {
        let mut st = self.st.lock().unwrap();
        if let Some(e) = st.registry.get_mut(route) {
            e["connected"] = json!(on);
            save_json(&self.cfg.routes_file, &st.registry);
        }
    }

    /// Deliver one router frame ({type: inbound|permission, params}) to a client, in the client's wire format.
    fn deliver(c: &Client, frame: &Value) {
        let line = if c.bridge {
            let method = if frame["type"] == "inbound" { "notifications/claude/channel" } else { "notifications/claude/channel/permission" };
            json!({ "jsonrpc": "2.0", "method": method, "params": frame["params"] }).to_string()
        } else {
            frame.to_string()
        };
        let _ = c.tx.send(line);
    }

    fn flush_queue(&self, route: &str) {
        let (c, q) = {
            let mut st = self.st.lock().unwrap();
            let Some(c) = st.clients.get(route).cloned() else { return };
            (c, st.queues.remove(route).unwrap_or_default())
        };
        for f in q {
            Self::deliver(&c, &f);
        }
    }

    fn router_dispatch(self: &Arc<Self>, info: &RouteInfo, mut params: Value) {
        {
            let meta = &mut params["meta"];
            meta["route"] = json!(info.route);
            if info.thread != 0 {
                meta["thread_id"] = json!(info.thread.to_string());
            }
            if let Some(t) = &info.chat_title {
                meta["chat_title"] = json!(safe_name(t));
            }
            if let Some(t) = &info.topic_title {
                meta["topic_title"] = json!(safe_name(t));
            }
        }
        self.touch(info);
        let frame = json!({ "type": "inbound", "params": params });
        let need_spawn = {
            let mut st = self.st.lock().unwrap();
            if let Some(c) = st.clients.get(&info.route).cloned() {
                if !st.booting.contains(&info.route) {
                    drop(st);
                    Self::deliver(&c, &frame);
                    return;
                }
            }
            st.queues.entry(info.route.clone()).or_default().push(frame);
            !st.spawning.contains_key(&info.route) && !st.booting.contains(&info.route)
        };
        if need_spawn {
            self.spawn_session(info);
        }
    }

    fn spawn_session(self: &Arc<Self>, info: &RouteInfo) {
        let Ok(cmd) = std::env::var("TELEGRAM_SPAWN_CMD") else {
            eprintln!("router: no TELEGRAM_SPAWN_CMD, cannot start {}", info.route);
            return;
        };
        eprintln!("router: starting session for {} ({})", info.route, info.topic_title.clone().or(info.chat_title.clone()).unwrap_or("?".into()));
        self.st.lock().unwrap().booting.insert(info.route.clone());
        let title = info.topic_title.clone().or(info.chat_title.clone()).unwrap_or_default();
        let app = self.clone();
        let route = info.route.clone();
        let (chat, thread) = (info.chat.clone(), info.thread);
        let child = tokio::process::Command::new("bash")
            .arg("-c")
            .arg(&cmd)
            .env("TG_ROUTE", &route)
            .env("TG_CHAT", &chat)
            .env("TG_THREAD", thread.to_string())
            .env("TG_TITLE", &title)
            .stdout(std::process::Stdio::null())
            .spawn();
        let app2 = app.clone();
        let route2 = route.clone();
        tokio::spawn(async move {
            if let Ok(mut c) = child {
                let _ = c.wait().await;
            }
            tokio::time::sleep(Duration::from_millis(1500)).await;
            app2.st.lock().unwrap().booting.remove(&route2);
            app2.flush_queue(&route2);
        });
        let h = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(150)).await;
            let missing = {
                let mut st = app.st.lock().unwrap();
                st.spawning.remove(&route);
                if st.clients.contains_key(&route) {
                    false
                } else {
                    st.queues.remove(&route);
                    true
                }
            };
            if missing {
                eprintln!("router: session for {route} did not connect in time");
                let mut extra = json!({});
                if thread != 0 {
                    extra["message_thread_id"] = json!(thread);
                }
                let _ = app.tg.send_message(&chat, "The session for this chat did not start in time. Send the message again in a minute.", extra).await;
            }
        });
        self.st.lock().unwrap().spawning.insert(info.route.clone(), h.abort_handle());
    }

    pub fn emit_permission(&self, request_id: &str, behavior: &str) {
        let frame = json!({ "type": "permission", "params": { "request_id": request_id, "behavior": behavior } });
        let targets: Vec<Client> = {
            let mut st = self.st.lock().unwrap();
            let owner = st.perm_owners.remove(request_id);
            match owner.and_then(|o| st.clients.get(&o).cloned()) {
                Some(c) => vec![c],
                None => st.clients.values().cloned().collect(),
            }
        };
        for c in targets {
            Self::deliver(&c, &frame);
        }
    }

    pub fn show_permission_prompt(self: &Arc<Self>, request_id: &str, tool: &str, desc: &str, input: &str) {
        self.st.lock().unwrap().pending_perms.insert(request_id.to_string(), (tool.into(), desc.into(), input.into()));
        let kb = json!({ "inline_keyboard": [[
            { "text": "See more", "callback_data": format!("perm:more:{request_id}") },
            { "text": "✅ Allow", "callback_data": format!("perm:allow:{request_id}") },
            { "text": "❌ Deny", "callback_data": format!("perm:deny:{request_id}") },
        ]]});
        for chat in self.access().allow_from {
            let app = self.clone();
            let kb = kb.clone();
            let text = format!("🔐 Permission: {tool}");
            tokio::spawn(async move {
                if let Err(e) = app.tg.send_message(&chat, &text, json!({ "reply_markup": kb })).await {
                    eprintln!("permission_request send to {chat} failed: {e}");
                }
            });
        }
    }
}

// ------------------------------------------------------------------ inbound updates

struct Attachment {
    kind: &'static str,
    file_id: String,
    size: Option<i64>,
    mime: Option<String>,
    name: Option<String>,
}

fn mentioned(app: &App, msg: &Value, extra: &[String]) -> bool {
    let bot = app.bot_username.lock().unwrap().to_lowercase();
    let ents = msg.get("entities").or(msg.get("caption_entities")).and_then(|e| e.as_array()).cloned().unwrap_or_default();
    let text = msg["text"].as_str().or(msg["caption"].as_str()).unwrap_or("");
    let u16: Vec<u16> = text.encode_utf16().collect();
    for e in &ents {
        match e["type"].as_str() {
            Some("mention") => {
                let (o, l) = (e["offset"].as_u64().unwrap_or(0) as usize, e["length"].as_u64().unwrap_or(0) as usize);
                if let Some(s) = u16.get(o..o + l) {
                    if String::from_utf16_lossy(s).to_lowercase() == format!("@{bot}") {
                        return true;
                    }
                }
            }
            Some("text_mention") => {
                if e["user"]["is_bot"].as_bool() == Some(true) && e["user"]["username"].as_str().map(|u| u.to_lowercase()) == Some(bot.clone()) {
                    return true;
                }
            }
            _ => {}
        }
    }
    if msg["reply_to_message"]["from"]["username"].as_str().map(|u| u.to_lowercase()) == Some(bot.clone()) && !bot.is_empty() {
        return true;
    }
    for p in extra {
        if let Ok(r) = Regex::new(&format!("(?i){p}")) {
            if r.is_match(text).unwrap_or(false) {
                return true;
            }
        }
    }
    false
}

fn gate(app: &App, msg: &Value, access: &Access) -> bool {
    if access.dm_policy == "disabled" || msg["from"].is_null() {
        return false;
    }
    let sender = msg["from"]["id"].to_string();
    match msg["chat"]["type"].as_str() {
        Some("private") => access.allow_from.contains(&sender),
        Some("group") | Some("supergroup") => {
            let Some(p) = access.groups.get(&msg["chat"]["id"].to_string()) else { return false };
            if !p.allow_from.is_empty() && !p.allow_from.contains(&sender) {
                return false;
            }
            !(p.require_mention && !mentioned(app, msg, &access.mention_patterns))
        }
        _ => false,
    }
}

fn dm_command_ok(app: &App, msg: &Value) -> bool {
    if msg["chat"]["type"].as_str() != Some("private") || msg["from"].is_null() {
        return false;
    }
    let a = app.access();
    a.dm_policy != "disabled" && !(a.dm_policy == "allowlist" && !a.allow_from.contains(&msg["from"]["id"].to_string()))
}

impl App {
    async fn handle_update(self: &Arc<Self>, u: Value) {
        if let Some(cq) = u.get("callback_query") {
            self.handle_callback(cq).await;
        } else if let Some(msg) = u.get("message") {
            self.handle_message(msg).await;
        }
    }

    async fn handle_message(self: &Arc<Self>, msg: &Value) {
        let chat = msg["chat"]["id"].to_string();
        // topic service messages
        for key in ["forum_topic_created", "forum_topic_edited"] {
            if msg.get(key).is_some() {
                let a = self.access();
                let sender = msg["from"]["id"].to_string();
                let allowed = a.groups.contains_key(&chat) || (msg["chat"]["type"] == "private" && a.allow_from.contains(&sender));
                if allowed {
                    let thread = if key == "forum_topic_created" { msg["message_thread_id"].as_i64().or(msg["message_id"].as_i64()) } else { msg["message_thread_id"].as_i64() };
                    self.learn_topic(&chat, thread.unwrap_or(0), msg[key]["name"].as_str());
                }
                return;
            }
        }
        // bot commands (DM only; silently consumed elsewhere)
        if let (Some(text), Some(first)) = (msg["text"].as_str(), msg["entities"].get(0)) {
            if first["type"] == "bot_command" && first["offset"] == 0 {
                let len = first["length"].as_u64().unwrap_or(0) as usize;
                let cmd: String = text.chars().take(len).collect();
                let name = cmd.trim_start_matches('/').split('@').next().unwrap_or("").to_string();
                if name == "model" || name == "cwd" {
                    if dm_command_ok(self, msg) {
                        self.handle_command(&name, msg).await;
                    }
                    return;
                }
            }
        }
        if let Some(t) = msg["text"].as_str() {
            self.handle_inbound(msg, t.to_string(), None, None).await;
        } else if let Some(photos) = msg["photo"].as_array() {
            let cap = msg["caption"].as_str().unwrap_or("(photo)").to_string();
            let best = photos.last().cloned().unwrap_or(Value::Null);
            self.handle_inbound(msg, cap, Some(best), None).await;
        } else if let Some(d) = msg.get("document") {
            let name = d["file_name"].as_str().map(safe_name);
            let text = msg["caption"].as_str().map(|s| s.to_string()).unwrap_or_else(|| format!("(document: {})", name.clone().unwrap_or("file".into())));
            let att = Attachment { kind: "document", file_id: d["file_id"].as_str().unwrap_or("").into(), size: d["file_size"].as_i64(), mime: d["mime_type"].as_str().map(|s| s.into()), name };
            self.handle_inbound(msg, text, None, Some(att)).await;
        } else if let Some(v) = msg.get("voice") {
            let text = msg["caption"].as_str().unwrap_or("(voice message)").to_string();
            let att = Attachment { kind: "voice", file_id: v["file_id"].as_str().unwrap_or("").into(), size: v["file_size"].as_i64(), mime: v["mime_type"].as_str().map(|s| s.into()), name: None };
            self.handle_inbound(msg, text, None, Some(att)).await;
        } else if let Some(a) = msg.get("audio") {
            let name = a["file_name"].as_str().map(safe_name);
            let title = a["title"].as_str().map(safe_name);
            let text = msg["caption"].as_str().map(|s| s.to_string()).unwrap_or_else(|| format!("(audio: {})", title.or(name.clone()).unwrap_or("audio".into())));
            let att = Attachment { kind: "audio", file_id: a["file_id"].as_str().unwrap_or("").into(), size: a["file_size"].as_i64(), mime: a["mime_type"].as_str().map(|s| s.into()), name };
            self.handle_inbound(msg, text, None, Some(att)).await;
        } else if let Some(v) = msg.get("video") {
            let text = msg["caption"].as_str().unwrap_or("(video)").to_string();
            let att = Attachment { kind: "video", file_id: v["file_id"].as_str().unwrap_or("").into(), size: v["file_size"].as_i64(), mime: v["mime_type"].as_str().map(|s| s.into()), name: v["file_name"].as_str().map(safe_name) };
            self.handle_inbound(msg, text, None, Some(att)).await;
        } else if let Some(v) = msg.get("video_note") {
            let att = Attachment { kind: "video_note", file_id: v["file_id"].as_str().unwrap_or("").into(), size: v["file_size"].as_i64(), mime: None, name: None };
            self.handle_inbound(msg, "(video note)".into(), None, Some(att)).await;
        } else if let Some(s) = msg.get("sticker") {
            let emoji = s["emoji"].as_str().map(|e| format!(" {e}")).unwrap_or_default();
            let att = Attachment { kind: "sticker", file_id: s["file_id"].as_str().unwrap_or("").into(), size: s["file_size"].as_i64(), mime: None, name: None };
            self.handle_inbound(msg, format!("(sticker{emoji})"), None, Some(att)).await;
        }
    }

    async fn download_photo(&self, best: &Value) -> Option<String> {
        let file = self.tg.call("getFile", json!({ "file_id": best["file_id"] })).await.ok()?;
        let fp = file["file_path"].as_str()?;
        let bytes = self.tg.download(fp).await.ok()?;
        let ext = fp.rsplit('.').next().filter(|e| *e != fp).unwrap_or("jpg");
        let path = self.cfg.inbox_dir.join(format!("{}-{}.{}", now_ms(), best["file_unique_id"].as_str().unwrap_or("p"), ext));
        std::fs::create_dir_all(&self.cfg.inbox_dir).ok()?;
        std::fs::write(&path, bytes).ok()?;
        Some(path.to_string_lossy().into_owned())
    }

    async fn handle_inbound(self: &Arc<Self>, msg: &Value, text: String, photo: Option<Value>, att: Option<Attachment>) {
        let access = self.access();
        if !gate(self, msg, &access) {
            return;
        }
        let chat_id = msg["chat"]["id"].to_string();
        let msg_id = msg["message_id"].as_i64();
        let thread_id = if msg["is_topic_message"].as_bool() == Some(true) { msg["message_thread_id"].as_i64() } else { None };
        let info = self.route_for(msg);

        static PERM: OnceLock<Regex> = OnceLock::new();
        if let Ok(Some(c)) = re(&PERM, r"(?i)^\s*(y|yes|n|no)\s+([a-km-z]{5})\s*$").captures(&text) {
            let yes = c[1].to_lowercase().starts_with('y');
            self.emit_permission(&c[2].to_lowercase(), if yes { "allow" } else { "deny" });
            if let Some(m) = msg_id {
                let (tg, ch) = (self.tg.clone(), chat_id.clone());
                tokio::spawn(async move {
                    let _ = tg.react(&ch, m, Some(if yes { "✅" } else { "❌" })).await;
                });
            }
            return;
        }

        // typing indicator + ack reactions (fire and forget)
        {
            let (tg, ch) = (self.tg.clone(), chat_id.clone());
            let mut b = json!({ "chat_id": ch, "action": "typing" });
            if let Some(t) = thread_id {
                b["message_thread_id"] = json!(t);
            }
            tokio::spawn(async move {
                let _ = tg.call("sendChatAction", b).await;
            });
        }
        if let (Some(e), Some(m)) = (access.ack_reaction.clone(), msg_id) {
            let (tg, ch) = (self.tg.clone(), chat_id.clone());
            tokio::spawn(async move {
                let _ = tg.react(&ch, m, Some(&e)).await;
            });
        }
        if msg["chat"]["type"] == "private" {
            if let Some(m) = msg_id {
                self.st.lock().unwrap().pending_ack.entry(info.route.clone()).or_default().push((chat_id.clone(), m));
                let (tg, ch) = (self.tg.clone(), chat_id.clone());
                tokio::spawn(async move {
                    let _ = tg.react(&ch, m, Some("👀")).await;
                });
            }
        }

        let image_path = match &photo {
            Some(p) => self.download_photo(p).await,
            None => None,
        };

        let reply_to = msg.get("reply_to_message").filter(|r| r.get("forum_topic_created").is_none());
        let reply_text = reply_to
            .map(|r| {
                let t = r["text"].as_str().or(r["caption"].as_str()).unwrap_or("");
                let t: String = t.split_whitespace().collect::<Vec<_>>().join(" ").chars().take(400).collect();
                if !t.is_empty() {
                    t
                } else if r.get("photo").is_some() {
                    "[photo]".into()
                } else if r.get("voice").is_some() {
                    "[voice]".into()
                } else if r.get("document").is_some() {
                    "[document]".into()
                } else {
                    String::new()
                }
            })
            .unwrap_or_default();

        let from = &msg["from"];
        let mut meta = Map::new();
        meta.insert("chat_id".into(), json!(chat_id));
        if let Some(m) = msg_id {
            meta.insert("message_id".into(), json!(m.to_string()));
        }
        if let Some(t) = thread_id {
            meta.insert("thread_id".into(), json!(t.to_string()));
        }
        meta.insert("user".into(), json!(from["username"].as_str().map(|s| s.to_string()).unwrap_or_else(|| from["id"].to_string())));
        meta.insert("user_id".into(), json!(from["id"].to_string()));
        meta.insert("ts".into(), json!(iso(msg["date"].as_i64().unwrap_or(0))));
        if let Some(r) = reply_to {
            meta.insert("reply_to_message_id".into(), json!(r["message_id"].to_string()));
            if !r["from"].is_null() {
                meta.insert("reply_to_user".into(), json!(r["from"]["username"].as_str().map(|s| s.to_string()).unwrap_or_else(|| r["from"]["id"].to_string())));
            }
            if !reply_text.is_empty() {
                meta.insert("reply_to_text".into(), json!(reply_text));
            }
        }
        if let Some(p) = image_path {
            meta.insert("image_path".into(), json!(p));
        }
        if let Some(a) = att {
            meta.insert("attachment_kind".into(), json!(a.kind));
            meta.insert("attachment_file_id".into(), json!(a.file_id));
            if let Some(s) = a.size {
                meta.insert("attachment_size".into(), json!(s.to_string()));
            }
            if let Some(m) = a.mime {
                meta.insert("attachment_mime".into(), json!(m));
            }
            if let Some(n) = a.name {
                meta.insert("attachment_name".into(), json!(n));
            }
        }
        self.router_dispatch(&info, json!({ "content": text, "meta": Value::Object(meta) }));
    }

    async fn handle_command(self: &Arc<Self>, name: &str, msg: &Value) {
        let chat = msg["chat"]["id"].to_string();
        let mut extra = json!({});
        if let Some(t) = msg["message_thread_id"].as_i64().filter(|t| *t != 0) {
            extra["message_thread_id"] = json!(t);
        }
        if name == "model" {
            let models = ["opus", "sonnet", "haiku", "fable", "default"];
            let efforts = ["low", "medium", "high", "xhigh", "max"];
            let row = |k: &str, xs: &[&str]| -> Vec<Value> { xs.iter().map(|x| json!({ "text": x, "callback_data": format!("mdl:{k}:{x}") })).collect() };
            extra["reply_markup"] = json!({ "inline_keyboard": [row("model", &models), row("effort", &efforts)] });
            let _ = self.tg.send_message(&chat, "Model (top row) or effort (bottom row) for this topic's session:", extra).await;
        } else {
            let script = self.cfg.tg_dir.join("list-cwds");
            let rows: Vec<Vec<String>> = match tokio::time::timeout(Duration::from_secs(10), tokio::process::Command::new(&script).output()).await {
                Ok(Ok(o)) => String::from_utf8_lossy(&o.stdout).trim().lines().filter(|l| !l.is_empty()).map(|l| l.split('\t').map(|s| s.to_string()).collect()).collect(),
                _ => vec![],
            };
            let home = std::env::var("HOME").unwrap_or_default();
            let mut kb: Vec<Value> = rows
                .iter()
                .filter(|r| r.len() >= 3)
                .map(|r| json!([{ "text": format!("{} ({})", r[1], r[2].replace(&home, "~")), "callback_data": format!("cwd:{}", r[0]) }]))
                .collect();
            kb.push(json!([{ "text": "default (~/pi-life)", "callback_data": "cwd:default" }]));
            extra["reply_markup"] = json!({ "inline_keyboard": kb });
            let _ = self.tg.send_message(&chat, "Move this topic's session to which working dir? It restarts the session (transcript is carried over).", extra).await;
        }
    }

    async fn run_script(&self, script: &PathBuf, args: &[String]) -> Result<String, ()> {
        let o = tokio::time::timeout(Duration::from_secs(20), tokio::process::Command::new(script).args(args).output()).await.map_err(|_| ())?.map_err(|_| ())?;
        if o.status.success() { Ok(String::from_utf8_lossy(&o.stdout).trim().to_string()) } else { Err(()) }
    }

    async fn handle_callback(self: &Arc<Self>, cq: &Value) {
        let data = cq["data"].as_str().unwrap_or("").to_string();
        let id = cq["id"].clone();
        let sender = cq["from"]["id"].to_string();
        let answer = |text: Option<String>| {
            let tg = self.tg.clone();
            let mut b = json!({ "callback_query_id": id });
            if let Some(t) = text {
                b["text"] = json!(t);
            }
            async move {
                let _ = tg.call("answerCallbackQuery", b).await;
            }
        };
        let chat = cq["message"]["chat"]["id"].to_string();
        let mid = cq["message"]["message_id"].clone();
        let edit = |text: String, kb: Option<Value>| {
            let tg = self.tg.clone();
            let mut b = json!({ "chat_id": chat, "message_id": mid, "text": text });
            if let Some(k) = kb {
                b["reply_markup"] = k;
            }
            async move {
                let _ = tg.call("editMessageText", b).await;
            }
        };
        let thread = cq["message"]["message_thread_id"].as_i64().unwrap_or(0);
        let route = if thread != 0 { format!("dm:{sender}:{thread}") } else { format!("dm:{sender}") };
        let home = std::env::var("HOME").unwrap_or_default();

        static CWD: OnceLock<Regex> = OnceLock::new();
        static MDL: OnceLock<Regex> = OnceLock::new();
        static PERM: OnceLock<Regex> = OnceLock::new();
        if let Ok(Some(c)) = re(&CWD, r"^cwd:(\w+)$").captures(&data) {
            if !self.access().allow_from.contains(&sender) {
                answer(Some("Not authorized.".into())).await;
                return;
            }
            match self.run_script(&self.cfg.tg_dir.join("set-cwd"), &[route, c[1].to_string()]).await {
                Ok(dir) => {
                    answer(Some("Restarting there…".into())).await;
                    edit(format!("Working dir → {}. Restarting the session; send a message in ~30s if it is quiet.", dir.replace(&home, "~")), None).await;
                }
                Err(_) => answer(Some("Failed (workspace gone?).".into())).await,
            }
            return;
        }
        if let Ok(Some(c)) = re(&MDL, r"^mdl:(model|effort):(\w+)$").captures(&data) {
            if !self.access().allow_from.contains(&sender) {
                answer(Some("Not authorized.".into())).await;
                return;
            }
            let script = std::env::var("TELEGRAM_MODEL_CMD").map(PathBuf::from).unwrap_or_else(|_| self.cfg.tg_dir.join("set-model"));
            match self.run_script(&script, &[route, c[1].to_string(), c[2].to_string()]).await {
                Ok(out) => {
                    answer(Some(format!("{} = {}: {}", &c[1], &c[2], out))).await;
                    edit(format!("{} → {} ({})", &c[1], &c[2], out), None).await;
                }
                Err(_) => answer(Some("Failed to set.".into())).await,
            }
            return;
        }
        let Ok(Some(m)) = re(&PERM, r"^perm:(allow|deny|more):([a-km-z]{5})$").captures(&data) else {
            answer(None).await;
            return;
        };
        if !self.access().allow_from.contains(&sender) {
            answer(Some("Not authorized.".into())).await;
            return;
        }
        let (behavior, request_id) = (m[1].to_string(), m[2].to_string());
        if behavior == "more" {
            let details = self.st.lock().unwrap().pending_perms.get(&request_id).cloned();
            let Some((tool, desc, input)) = details else {
                answer(Some("Details no longer available.".into())).await;
                return;
            };
            let pretty = serde_json::from_str::<Value>(&input).ok().and_then(|v| serde_json::to_string_pretty(&v).ok()).unwrap_or(input);
            let kb = json!({ "inline_keyboard": [[
                { "text": "✅ Allow", "callback_data": format!("perm:allow:{request_id}") },
                { "text": "❌ Deny", "callback_data": format!("perm:deny:{request_id}") },
            ]]});
            edit(format!("🔐 Permission: {tool}\n\ntool_name: {tool}\ndescription: {desc}\ninput_preview:\n{pretty}"), Some(kb)).await;
            answer(None).await;
            return;
        }
        self.emit_permission(&request_id, &behavior);
        self.st.lock().unwrap().pending_perms.remove(&request_id);
        let label = if behavior == "allow" { "✅ Allowed" } else { "❌ Denied" };
        answer(Some(label.into())).await;
        if let Some(t) = cq["message"]["text"].as_str().filter(|t| !t.is_empty()) {
            edit(format!("{t}\n\n{label}"), None).await;
        }
    }
}

// ------------------------------------------------------------------ socket server

impl App {
    async fn serve_conn(self: Arc<Self>, stream: tokio::net::UnixStream) {
        let conn = {
            let mut st = self.st.lock().unwrap();
            st.next_conn += 1;
            st.next_conn
        };
        let (rd, mut wr) = stream.into_split();
        let (tx, mut rx) = unbounded_channel::<String>();
        let writer = tokio::spawn(async move {
            while let Some(l) = rx.recv().await {
                if wr.write_all(l.as_bytes()).await.is_err() || wr.write_all(b"\n").await.is_err() {
                    break;
                }
            }
            let _ = wr.shutdown().await;
        });
        let mut lines = BufReader::new(rd).lines();
        let mut route: Option<String> = None;
        let mut mcp_session: Option<Arc<mcp::Session>> = None;
        let mut owner = false;
        while let Ok(Some(line)) = lines.next_line().await {
            let Ok(f) = serde_json::from_str::<Value>(&line) else { continue };
            if let Some(s) = &mcp_session {
                s.handle(&self, f);
                continue;
            }
            match f["type"].as_str() {
                Some("bridge") if f["route"].is_string() => {
                    let r = f["route"].as_str().unwrap().to_string();
                    let admin = f["admin"].as_bool() == Some(true);
                    mcp_session = Some(Arc::new(mcp::Session::new(r.clone(), admin, tx.clone())));
                    route = Some(r.clone());
                    let first = {
                        let mut st = self.st.lock().unwrap();
                        if st.clients.contains_key(&r) {
                            false
                        } else {
                            st.clients.insert(r.clone(), Client { conn, tx: tx.clone(), bridge: true });
                            true
                        }
                    };
                    owner = first;
                    if first {
                        self.attach_done(&r, "bridge");
                    } else {
                        eprintln!("router: duplicate bridge for {r} attached without inbound");
                    }
                }
                Some("hello") if f["route"].is_string() => {
                    let r = f["route"].as_str().unwrap().to_string();
                    let dup = {
                        let mut st = self.st.lock().unwrap();
                        if st.clients.contains_key(&r) {
                            true
                        } else {
                            st.clients.insert(r.clone(), Client { conn, tx: tx.clone(), bridge: false });
                            false
                        }
                    };
                    if dup {
                        eprintln!("router: duplicate client for {r} rejected");
                        break;
                    }
                    route = Some(r.clone());
                    owner = true;
                    self.attach_done(&r, "client");
                }
                Some("turn_done") if f["route"].is_string() => {
                    let r = f["route"].as_str().unwrap().to_string();
                    let list = self.st.lock().unwrap().pending_ack.remove(&r).unwrap_or_default();
                    for (chat, m) in list {
                        let tg = self.tg.clone();
                        tokio::spawn(async move {
                            let _ = tg.react(&chat, m, None).await;
                        });
                    }
                    break;
                }
                Some("reacted") if route.is_some() => {
                    let (chat, m) = (f["chat_id"].to_string().trim_matches('"').to_string(), f["message_id"].as_str().and_then(|s| s.parse::<i64>().ok()).or(f["message_id"].as_i64()).unwrap_or(0));
                    self.clear_ack(route.as_ref().unwrap(), &chat, m);
                }
                Some("perm_request") if route.is_some() => {
                    let p = &f["params"];
                    let rid = p["request_id"].as_str().unwrap_or("").to_string();
                    self.st.lock().unwrap().perm_owners.insert(rid.clone(), route.clone().unwrap());
                    self.show_permission_prompt(&rid, p["tool_name"].as_str().unwrap_or(""), p["description"].as_str().unwrap_or(""), p["input_preview"].as_str().unwrap_or(""));
                }
                _ => {}
            }
        }
        // disconnected
        if let Some(r) = route {
            let removed = {
                let mut st = self.st.lock().unwrap();
                if owner && st.clients.get(&r).map(|c| c.conn) == Some(conn) {
                    st.clients.remove(&r);
                    true
                } else {
                    false
                }
            };
            if removed {
                self.set_connected(&r, false);
                eprintln!("router: client for {r} disconnected");
            }
        }
        drop(tx);
        writer.abort();
    }

    fn attach_done(&self, route: &str, kind: &str) {
        let t = self.st.lock().unwrap().spawning.remove(route);
        if let Some(t) = t {
            t.abort();
        }
        self.set_connected(route, true);
        eprintln!("router: {kind} connected for {route}");
        if !self.st.lock().unwrap().booting.contains(route) {
            self.flush_queue(route);
        }
    }

    pub fn clear_ack(&self, route: &str, chat: &str, msg: i64) {
        let mut st = self.st.lock().unwrap();
        if let Some(l) = st.pending_ack.get_mut(route) {
            l.retain(|(c, m)| !(c == chat && *m == msg));
        }
    }
}

// ------------------------------------------------------------------ main

fn load_env_file(path: &PathBuf) {
    let Ok(s) = std::fs::read_to_string(path) else { return };
    for l in s.lines() {
        if let Some((k, v)) = l.split_once('=') {
            if k.chars().all(|c| c.is_alphanumeric() || c == '_') && !k.is_empty() && std::env::var_os(k).is_none() {
                std::env::set_var(k, v);
            }
        }
    }
}

fn main() {
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("runtime");
    rt.block_on(run());
}

async fn run() {
    let home = PathBuf::from(std::env::var("HOME").unwrap_or_else(|_| ".".into()));
    let state_dir = std::env::var("TELEGRAM_STATE_DIR").map(PathBuf::from).unwrap_or_else(|_| {
        std::env::var("CLAUDE_CONFIG_DIR").map(PathBuf::from).unwrap_or_else(|_| home.join(".claude")).join("channels").join("telegram")
    });
    let _ = std::fs::create_dir_all(&state_dir);
    load_env_file(&state_dir.join(".env"));
    let Ok(token) = std::env::var("TELEGRAM_BOT_TOKEN") else {
        eprintln!("telegram channel: TELEGRAM_BOT_TOKEN required (set in {}/.env)", state_dir.display());
        std::process::exit(1);
    };
    let api_root = std::env::var("TELEGRAM_API_ROOT").unwrap_or_else(|_| "https://api.telegram.org".into());
    let access_file = state_dir.join("access.json");
    let is_static = std::env::var("TELEGRAM_ACCESS_MODE").as_deref() == Ok("static");
    let cfg = Cfg {
        inbox_dir: state_dir.join("inbox"),
        sock: std::env::var("TELEGRAM_ROUTER_SOCK").map(PathBuf::from).unwrap_or_else(|_| state_dir.join("router.sock")),
        topics_file: state_dir.join("topics.json"),
        routes_file: std::env::var("TELEGRAM_ROUTES_FILE").map(PathBuf::from).unwrap_or_else(|_| state_dir.join("routes.json")),
        access_file: access_file.clone(),
        is_static,
        tg_dir: home.join("pi-life/scripts/semar/telegram-session"),
        token: token.clone(),
        state_dir: state_dir.clone(),
    };
    let load_map = |p: &PathBuf| -> Map<String, Value> {
        std::fs::read_to_string(p).ok().and_then(|s| serde_json::from_str::<Value>(&s).ok()).and_then(|v| v.as_object().cloned()).unwrap_or_default()
    };
    let st = State { topics: load_map(&cfg.topics_file), registry: load_map(&cfg.routes_file), ..Default::default() };
    let app = Arc::new(App {
        tg: tg::Tg::new(&api_root, &token),
        boot_access: if is_static { Some(Access::load(&access_file)) } else { None },
        bot_username: Mutex::new(String::new()),
        st: Mutex::new(st),
        cfg,
    });

    // socket server
    let _ = std::fs::remove_file(&app.cfg.sock);
    let listener = UnixListener::bind(&app.cfg.sock).expect("bind router socket");
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&app.cfg.sock, std::fs::Permissions::from_mode(0o600));
    }
    {
        let app = app.clone();
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((s, _)) => {
                        tokio::spawn(app.clone().serve_conn(s));
                    }
                    Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
                }
            }
        });
    }

    // reap idle non-DM sessions
    if let Ok(reap) = std::env::var("TELEGRAM_REAP_CMD") {
        let mins: u64 = std::env::var("TELEGRAM_REAP_MINUTES").ok().and_then(|s| s.parse().ok()).unwrap_or(360);
        let app = app.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(300)).await;
                let due: Vec<String> = {
                    let st = app.st.lock().unwrap();
                    st.registry
                        .iter()
                        .filter(|(r, v)| {
                            let dm_main = r.strip_prefix("dm:").map(|x| x.chars().all(|c| c.is_ascii_digit())).unwrap_or(false);
                            !dm_main && st.clients.contains_key(*r) && now_ms().saturating_sub(v["lastActive"].as_u64().unwrap_or(0)) >= mins * 60_000
                        })
                        .map(|(r, _)| r.clone())
                        .collect()
                };
                for r in due {
                    let _ = tokio::process::Command::new("bash").arg("-c").arg(&reap).env("TG_ROUTE", r).stdout(std::process::Stdio::null()).spawn();
                }
            }
        });
    }

    // clean shutdown
    {
        let sock = app.cfg.sock.clone();
        tokio::spawn(async move {
            let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
            tokio::select! { _ = term.recv() => {}, _ = tokio::signal::ctrl_c() => {} }
            eprintln!("telegram channel: shutting down");
            let _ = std::fs::remove_file(sock);
            std::process::exit(0);
        });
    }

    // long-poll loop
    let mut offset: i64 = 0;
    let mut attempt: u64 = 0;
    loop {
        if app.bot_username.lock().unwrap().is_empty() {
            match app.tg.call("getMe", json!({})).await {
                Ok(me) => {
                    let u = me["username"].as_str().unwrap_or("").to_string();
                    eprintln!("telegram channel: polling as @{u}");
                    *app.bot_username.lock().unwrap() = u;
                    attempt = 0;
                    let _ = app
                        .tg
                        .call("setMyCommands", json!({
                            "commands": [
                                { "command": "model", "description": "Change model or effort for this topic" },
                                { "command": "cwd", "description": "Move this topic to another herdr workspace dir" },
                            ],
                            "scope": { "type": "all_private_chats" },
                        }))
                        .await;
                }
                Err(e) => {
                    attempt += 1;
                    if attempt >= 8 && e.contains("409") {
                        eprintln!("telegram channel: 409 Conflict persists after {attempt} attempts — another poller holds the token. Exiting.");
                        std::process::exit(1);
                    }
                    let delay = (1000 * attempt).min(15000);
                    eprintln!("telegram channel: polling error: {e}, retrying in {}s", delay / 1000);
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                    continue;
                }
            }
        }
        match app.tg.call_t("getUpdates", json!({ "offset": offset, "timeout": 30 }), 45).await {
            Ok(v) => {
                attempt = 0;
                for u in v.as_array().cloned().unwrap_or_default() {
                    offset = offset.max(u["update_id"].as_i64().unwrap_or(0) + 1);
                    app.handle_update(u).await;
                }
            }
            Err(e) => {
                attempt += 1;
                if e.contains("409") && attempt >= 8 {
                    eprintln!("telegram channel: 409 Conflict persists after {attempt} attempts — another poller holds the token. Exiting.");
                    std::process::exit(1);
                }
                let delay = (1000 * attempt).min(15000);
                eprintln!("telegram channel: {}, retrying in {}s", if e.contains("409") { "409 Conflict — another instance is polling".to_string() } else { format!("polling error: {e}") }, delay / 1000);
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }
        }
    }
}
