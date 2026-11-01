//! Markdown -> Telegram MarkdownV2 conversion and message chunking (ported from server.ts, same semantics).
use fancy_regex::Regex;
use std::sync::OnceLock;

const V2_SPECIAL: &str = "_*[]()~`>#+-=|{}.!\\";

pub fn v2esc(s: &str) -> String {
    let mut o = String::with_capacity(s.len() + 8);
    for c in s.chars() {
        if V2_SPECIAL.contains(c) {
            o.push('\\');
        }
        o.push(c);
    }
    o
}

fn esc_code(s: &str) -> String {
    let mut o = String::with_capacity(s.len() + 4);
    for c in s.chars() {
        if c == '`' || c == '\\' {
            o.push('\\');
        }
        o.push(c);
    }
    o
}

fn esc_url(s: &str) -> String {
    let mut o = String::with_capacity(s.len() + 4);
    for c in s.chars() {
        if c == ')' || c == '\\' {
            o.push('\\');
        }
        o.push(c);
    }
    o
}

fn md_re() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    R.get_or_init(|| {
        Regex::new(concat!(
            r"(?m)",
            r"```[^\n`]*\n?([\s\S]*?)```",
            r"|`([^`\n]+)`",
            r"|\[([^\]\n]+)\]\((https?://[^)\s]+)\)",
            r"|^(#{1,6})[ \t]+(.+)$",
            r"|^([ \t]*)[-*][ \t]+",
            r"|\*\*(?=\S)(.+?)(?<=\S)\*\*",
            r"|(?<![A-Za-z0-9_*])\*(?=[^\s*])([^*\n]+?)(?<=[^\s*])\*(?![A-Za-z0-9_*])",
            r"|(?<![A-Za-z0-9_])_(?=[^\s_])([^_\n]+?)(?<=[^\s_])_(?![A-Za-z0-9_])",
        ))
        .expect("md regex")
    })
}

pub fn md_to_v2(src: &str) -> String {
    let mut out = String::new();
    let mut last = 0usize;
    for m in md_re().captures_iter(src) {
        let Ok(c) = m else { continue };
        let whole = c.get(0).unwrap();
        out.push_str(&v2esc(&src[last..whole.start()]));
        last = whole.end();
        if let Some(g) = c.get(1) {
            let body = esc_code(g.as_str());
            let body = body.strip_suffix('\n').unwrap_or(&body);
            out.push_str("```\n");
            out.push_str(body);
            out.push_str("\n```");
        } else if let Some(g) = c.get(2) {
            out.push('`');
            out.push_str(&esc_code(g.as_str()));
            out.push('`');
        } else if let Some(g) = c.get(3) {
            out.push_str(&format!("[{}]({})", v2esc(g.as_str()), esc_url(c.get(4).unwrap().as_str())));
        } else if let Some(g) = c.get(6) {
            out.push_str(&format!("*{}*", v2esc(g.as_str())));
        } else if let Some(g) = c.get(7) {
            out.push_str(g.as_str());
            out.push_str("• ");
        } else if let Some(g) = c.get(8) {
            out.push_str(&format!("*{}*", v2esc(g.as_str())));
        } else if let Some(g) = c.get(9) {
            out.push_str(&format!("*{}*", v2esc(g.as_str())));
        } else if let Some(g) = c.get(10) {
            out.push_str(&format!("_{}_", v2esc(g.as_str())));
        }
    }
    out.push_str(&v2esc(&src[last..]));
    out
}

fn code_span_re() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    R.get_or_init(|| Regex::new(r"```[\s\S]*?```|`[^`\n]*`").unwrap())
}
fn escaped_re() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    R.get_or_init(|| Regex::new(r"\\[_*\[\]()~`>#+\-=|{}.!]").unwrap())
}
fn unescape_re() -> &'static Regex {
    static R: OnceLock<Regex> = OnceLock::new();
    R.get_or_init(|| Regex::new(r"\\([_*\[\]()~`>#+\-=|{}.!\\])").unwrap())
}

/// Agents sometimes hand-escape MarkdownV2 without setting format; escaping again would show literal backslashes.
pub fn looks_v2_escaped(t: &str) -> bool {
    let stripped = code_span_re().replace_all(t, "");
    escaped_re().is_match(&stripped).unwrap_or(false)
}

pub fn unescape_v2(t: &str) -> String {
    unescape_re().replace_all(t, "$1").into_owned()
}

/// What to send for a given mode: 'markdownv2'/'plain' and already-escaped input pass through untouched.
pub fn prep_v2(t: &str, format: &str) -> String {
    if format == "markdownv2" || format == "plain" || looks_v2_escaped(t) {
        t.to_string()
    } else {
        md_to_v2(t)
    }
}

/// Largest index i <= max_start with chars[i..] starting with `pat`.
fn rfind_at_most(chars: &[char], pat: &[char], max_start: usize) -> Option<usize> {
    if pat.len() > chars.len() {
        return None;
    }
    let top = max_start.min(chars.len() - pat.len());
    (0..=top).rev().find(|&i| chars[i..i + pat.len()] == *pat)
}

pub fn chunk(text: &str, limit: usize, mode_newline: bool) -> Vec<String> {
    let mut rest: Vec<char> = text.chars().collect();
    if rest.len() <= limit {
        return vec![text.to_string()];
    }
    let mut out = Vec::new();
    while rest.len() > limit {
        let mut cut = limit;
        if mode_newline {
            let para = rfind_at_most(&rest, &['\n', '\n'], limit);
            let line = rfind_at_most(&rest, &['\n'], limit);
            let space = rfind_at_most(&rest, &[' '], limit);
            let half = limit as f64 / 2.0;
            cut = match (para, line, space) {
                (Some(p), _, _) if p as f64 > half => p,
                (_, Some(l), _) if l as f64 > half => l,
                (_, _, Some(s)) if s > 0 => s,
                _ => limit,
            };
        }
        out.push(rest[..cut].iter().collect::<String>());
        let mut tail: Vec<char> = rest[cut..].to_vec();
        let skip = tail.iter().take_while(|c| **c == '\n').count();
        tail.drain(..skip);
        rest = tail;
    }
    if !rest.is_empty() {
        out.push(rest.iter().collect());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn converts() {
        assert_eq!(md_to_v2("**bold** and _it_ `c` - x"), "*bold* and _it_ `c` \\- x");
        assert_eq!(md_to_v2("- item\n- two"), "• item\n• two");
        assert_eq!(md_to_v2("[t](https://a.b/c)"), "[t](https://a.b/c)");
        assert_eq!(md_to_v2("a.b (c)"), "a\\.b \\(c\\)");
        assert_eq!(md_to_v2("snake_case_name"), "snake\\_case\\_name");
        assert_eq!(md_to_v2("```\ncode.x\n```"), "```\ncode.x\n```");
        assert_eq!(md_to_v2("# Head"), "*Head*");
    }
    #[test]
    fn escaped_detection() {
        assert!(looks_v2_escaped("hello\\. world"));
        assert!(!looks_v2_escaped("`a\\.b` plain"));
        assert_eq!(unescape_v2("a\\.b\\\\c"), "a.b\\c");
    }
    #[test]
    fn chunks() {
        let t = "a".repeat(10);
        assert_eq!(chunk(&t, 4, false), vec!["aaaa", "aaaa", "aa"]);
        let p = format!("{}\n\n{}", "a".repeat(6), "b".repeat(6));
        assert_eq!(chunk(&p, 8, true), vec!["aaaaaa".to_string(), "bbbbbb".to_string()]);
    }
}
