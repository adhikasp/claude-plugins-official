//! Minimal Telegram Bot API client.
use reqwest::multipart;
use serde_json::{json, Value};
use std::time::Duration;

#[derive(Clone)]
pub struct Tg {
    http: reqwest::Client,
    base: String,
    file_base: String,
}

impl Tg {
    pub fn new(api_root: &str, token: &str) -> Tg {
        Tg {
            http: reqwest::Client::builder().pool_idle_timeout(Duration::from_secs(60)).build().expect("http client"),
            base: format!("{api_root}/bot{token}"),
            file_base: format!("{api_root}/file/bot{token}"),
        }
    }

    fn parse(method: &str, v: Value) -> Result<Value, String> {
        if v["ok"].as_bool() == Some(true) {
            Ok(v["result"].clone())
        } else {
            Err(format!(
                "Call to '{}' failed! ({}: {})",
                method,
                v["error_code"].as_i64().unwrap_or(0),
                v["description"].as_str().unwrap_or("unknown error")
            ))
        }
    }

    pub async fn call(&self, method: &str, body: Value) -> Result<Value, String> {
        self.call_t(method, body, 30).await
    }

    pub async fn call_t(&self, method: &str, body: Value, secs: u64) -> Result<Value, String> {
        let r = self
            .http
            .post(format!("{}/{}", self.base, method))
            .timeout(Duration::from_secs(secs))
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("Network request for '{method}' failed! ({e})"))?;
        let v: Value = r.json().await.map_err(|e| format!("bad response from '{method}': {e}"))?;
        Self::parse(method, v)
    }

    /// Send a local file via multipart. `fields` are plain string params (objects must already be JSON strings).
    pub async fn upload(&self, method: &str, field: &str, path: &str, fields: Vec<(String, String)>) -> Result<Value, String> {
        let bytes = tokio::fs::read(path).await.map_err(|e| format!("read {path}: {e}"))?;
        let name = std::path::Path::new(path).file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_else(|| "file".into());
        let mut form = multipart::Form::new();
        for (k, v) in fields {
            form = form.text(k, v);
        }
        form = form.part(field.to_string(), multipart::Part::bytes(bytes).file_name(name));
        let r = self
            .http
            .post(format!("{}/{}", self.base, method))
            .timeout(Duration::from_secs(300))
            .multipart(form)
            .send()
            .await
            .map_err(|e| format!("Network request for '{method}' failed! ({e})"))?;
        let v: Value = r.json().await.map_err(|e| format!("bad response from '{method}': {e}"))?;
        Self::parse(method, v)
    }

    pub async fn download(&self, file_path: &str) -> Result<Vec<u8>, String> {
        let r = self
            .http
            .get(format!("{}/{}", self.file_base, file_path))
            .timeout(Duration::from_secs(120))
            .send()
            .await
            .map_err(|e| format!("download failed: {e}"))?;
        if !r.status().is_success() {
            return Err(format!("download failed: HTTP {}", r.status().as_u16()));
        }
        r.bytes().await.map(|b| b.to_vec()).map_err(|e| format!("download failed: {e}"))
    }

    pub async fn send_message(&self, chat: &str, text: &str, extra: Value) -> Result<Value, String> {
        let mut b = json!({ "chat_id": chat, "text": text });
        if let (Some(o), Some(e)) = (b.as_object_mut(), extra.as_object()) {
            for (k, v) in e {
                o.insert(k.clone(), v.clone());
            }
        }
        self.call("sendMessage", b).await
    }

    pub async fn react(&self, chat: &str, msg: i64, emoji: Option<&str>) -> Result<Value, String> {
        let reaction = match emoji {
            Some(e) => json!([{ "type": "emoji", "emoji": e }]),
            None => json!([]),
        };
        self.call("setMessageReaction", json!({ "chat_id": chat, "message_id": msg, "reaction": reaction })).await
    }
}
