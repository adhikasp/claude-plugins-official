//! access.json handling (static allowlist semantics; pairing is not supported in the Rust router).
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;

#[derive(Clone, Default)]
pub struct Group {
    pub require_mention: bool,
    pub allow_from: Vec<String>,
}

#[derive(Clone)]
pub struct Access {
    pub dm_policy: String,
    pub allow_from: Vec<String>,
    pub groups: HashMap<String, Group>,
    pub mention_patterns: Vec<String>,
    pub ack_reaction: Option<String>,
    pub reply_to_mode: String,
    pub text_chunk_limit: Option<usize>,
    pub chunk_newline: bool,
}

fn strs(v: &Value) -> Vec<String> {
    v.as_array()
        .map(|a| a.iter().map(|x| x.as_str().map(|s| s.to_string()).unwrap_or_else(|| x.to_string())).collect())
        .unwrap_or_default()
}

impl Access {
    pub fn default_deny() -> Access {
        Access {
            dm_policy: "allowlist".into(),
            allow_from: vec![],
            groups: HashMap::new(),
            mention_patterns: vec![],
            ack_reaction: None,
            reply_to_mode: "first".into(),
            text_chunk_limit: None,
            chunk_newline: false,
        }
    }

    pub fn load(file: &Path) -> Access {
        let Ok(raw) = std::fs::read_to_string(file) else { return Access::default_deny() };
        let Ok(v) = serde_json::from_str::<Value>(&raw) else {
            eprintln!("telegram channel: access.json is corrupt, using deny-all");
            return Access::default_deny();
        };
        let mut groups = HashMap::new();
        if let Some(o) = v["groups"].as_object() {
            for (k, g) in o {
                groups.insert(
                    k.clone(),
                    Group { require_mention: g["requireMention"].as_bool().unwrap_or(true), allow_from: strs(&g["allowFrom"]) },
                );
            }
        }
        let mut dm_policy = v["dmPolicy"].as_str().unwrap_or("pairing").to_string();
        if dm_policy == "pairing" {
            dm_policy = "allowlist".into(); // pairing needs runtime writes; the router treats it as allowlist
        }
        Access {
            dm_policy,
            allow_from: strs(&v["allowFrom"]),
            groups,
            mention_patterns: strs(&v["mentionPatterns"]),
            ack_reaction: v["ackReaction"].as_str().filter(|s| !s.is_empty()).map(|s| s.to_string()),
            reply_to_mode: v["replyToMode"].as_str().unwrap_or("first").to_string(),
            text_chunk_limit: v["textChunkLimit"].as_u64().map(|n| n as usize),
            chunk_newline: v["chunkMode"].as_str() == Some("newline"),
        }
    }
}
