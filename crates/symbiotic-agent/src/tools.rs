use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
};

use regex::Regex;
use serde::Deserialize;
use serde_json::{Value, json};
use symbiotic_model::responses::FunctionTool;

use crate::policy::Policy;

pub(crate) fn safe_relative(path: &str) -> Result<(), String> {
    if path.contains(['\\', '\0'])
        || path.split('/').any(|part| matches!(part, "" | "." | ".."))
        || Path::new(path).is_absolute()
    {
        return Err("path must be relative with no empty, . or .. component".into());
    }
    Ok(())
}

pub(crate) struct Tools {
    root: PathBuf,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Read {
    path: String,
    start: usize,
    end: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Search {
    pattern: String,
    #[serde(deserialize_with = "crate::policy::present")]
    path: Option<String>,
    #[serde(deserialize_with = "crate::policy::present")]
    max_hits: Option<usize>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct List {
    path: String,
}

pub(crate) fn arguments<T: serde::de::DeserializeOwned>(text: &str) -> Result<T, String> {
    serde_json::from_str(text).map_err(|e| format!("invalid arguments: {e}"))
}

impl Tools {
    pub fn new(root: &Path) -> Result<Self, String> {
        let root = fs::canonicalize(root).map_err(|e| format!("root: {e}"))?;
        if !root.is_dir() {
            return Err("root must be a directory".into());
        }
        Ok(Self { root })
    }

    fn resolve(&self, path: &str) -> Result<PathBuf, String> {
        safe_relative(path)?;
        let resolved =
            fs::canonicalize(self.root.join(path)).map_err(|e| format!("{path}: {e}"))?;
        if !resolved.starts_with(&self.root) {
            return Err("path leaves root".into());
        }
        Ok(resolved)
    }

    pub fn run(&self, name: &str, text: &str, policy: &Policy) -> Result<String, String> {
        match name {
            "read" => self.read(arguments(text)?, policy.read_max_lines),
            "search" => self.search(arguments(text)?, policy.search_max_hits),
            "list" => self.list(arguments(text)?),
            _ => Err(format!("unknown tool: {name}")),
        }
    }

    fn read(&self, args: Read, max_lines: usize) -> Result<String, String> {
        if args.start < 1 || args.start > args.end {
            return Err("read requires 1 <= start <= end".into());
        }
        let text = fs::read_to_string(self.resolve(&args.path)?)
            .map_err(|e| format!("{}: {e}", args.path))?;
        let lines: Vec<_> = text.lines().collect();
        let end = args.end.min(lines.len());
        if args.start > end {
            return Err("read start is beyond the last line".into());
        }
        let shown_end = end.min(args.start.saturating_add(max_lines - 1));
        let mut result = format!(
            "{}: lines {}-{} of {}\n",
            args.path,
            args.start,
            shown_end,
            lines.len()
        );
        for number in args.start..=shown_end {
            result.push_str(&format!("{number}| {}\n", lines[number - 1]));
        }
        if shown_end < end {
            result.push_str(&format!(
                "[cut: read_max_lines {max_lines}; requested through line {end}]\n"
            ));
        }
        Ok(result)
    }

    fn list(&self, args: List) -> Result<String, String> {
        let mut names = Vec::new();
        for entry in fs::read_dir(self.resolve(&args.path)?).map_err(|e| e.to_string())? {
            let entry = entry.map_err(|e| e.to_string())?;
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| "directory entry is not UTF-8")?;
            // Directory-entry types do not follow links or inspect outside targets.
            let kind = entry.file_type().map_err(|e| e.to_string())?;
            names.push(format!("{name}{}", if kind.is_dir() { "/" } else { "" }));
        }
        names.sort();
        Ok(names.join("\n"))
    }

    fn search(&self, args: Search, max_hits: usize) -> Result<String, String> {
        let pattern = Regex::new(&args.pattern).map_err(|e| format!("invalid regex: {e}"))?;
        let limit = args.max_hits.unwrap_or(max_hits).min(max_hits);
        if limit == 0 {
            return Err("max_hits must be positive or null".into());
        }
        let (relative, start) = match args.path {
            Some(relative) => {
                let start = self.resolve(&relative)?;
                (relative, start)
            }
            None => (String::new(), self.root.clone()),
        };
        let mut hits = Vec::new();
        self.walk(
            &start,
            &relative,
            &pattern,
            limit,
            &mut hits,
            &mut HashSet::new(),
        )?;
        let mut result = hits.join("\n");
        if hits.len() == limit {
            result.push_str("\n[cut: search hit limit reached]");
        }
        Ok(result)
    }

    fn walk(
        &self,
        path: &Path,
        relative: &str,
        pattern: &Regex,
        limit: usize,
        hits: &mut Vec<String>,
        ancestors: &mut HashSet<PathBuf>,
    ) -> Result<(), String> {
        if path.is_dir() {
            if path.file_name().is_some_and(|name| name == ".git")
                || relative.rsplit('/').next() == Some(".git")
            {
                return Ok(());
            }
            let canonical = fs::canonicalize(path).map_err(|e| e.to_string())?;
            if !ancestors.insert(canonical.clone()) {
                return Ok(());
            }
            let mut entries = fs::read_dir(path)
                .map_err(|e| e.to_string())?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| e.to_string())?;
            entries.sort_by_key(|entry| entry.file_name());
            for entry in entries {
                let name = entry
                    .file_name()
                    .into_string()
                    .map_err(|_| "directory entry is not UTF-8")?;
                let child_relative = if relative.is_empty() {
                    name
                } else {
                    format!("{relative}/{name}")
                };
                let resolved = fs::canonicalize(entry.path()).map_err(|e| e.to_string())?;
                if !resolved.starts_with(&self.root) {
                    continue;
                }
                self.walk(&resolved, &child_relative, pattern, limit, hits, ancestors)?;
                if hits.len() == limit {
                    break;
                }
            }
            ancestors.remove(&canonical);
        } else if path.is_file() {
            let bytes = fs::read(path).map_err(|e| e.to_string())?;
            if bytes[..bytes.len().min(8192)].contains(&0) {
                return Ok(());
            }
            let text = std::str::from_utf8(&bytes).map_err(|e| format!("{relative}: {e}"))?;
            for (index, line) in text.lines().enumerate() {
                if pattern.is_match(line) {
                    hits.push(format!("{relative}:{}: {line}", index + 1));
                    if hits.len() == limit {
                        break;
                    }
                }
            }
        } else {
            return Err(format!("{relative}: not a regular file or directory"));
        }
        Ok(())
    }
}

pub(crate) fn object(properties: Value) -> Value {
    let required: Vec<_> = properties
        .as_object()
        .into_iter()
        .flat_map(|p| p.keys().cloned())
        .collect();
    json!({"type":"object", "properties":properties, "required":required, "additionalProperties":false})
}

pub(crate) fn definitions(report_schema: &Value, final_step: bool) -> Vec<FunctionTool> {
    let mut tools = Vec::new();
    if !final_step {
        for (name, description, parameters) in [
            (
                "read",
                "Read inclusive numbered lines of a file.",
                object(
                    json!({"path":{"type":"string"},"start":{"type":"integer"},"end":{"type":"integer"}}),
                ),
            ),
            (
                "search",
                "Search a regex; null path searches the root; null max_hits uses the policy limit.",
                object(
                    json!({"pattern":{"type":"string"},"path":{"type":["string","null"]},"max_hits":{"type":["integer","null"]}}),
                ),
            ),
            (
                "list",
                "List sorted directory entries.",
                object(json!({"path":{"type":"string"}})),
            ),
            (
                "note",
                "Add a note with syntactically checked references.",
                object(
                    json!({"kind":{"type":"string","enum":["covered","finding","question","plan"]},"text":{"type":"string"},"refs":{"type":"array","items":object(json!({"path":{"type":"string"},"ranges":{"type":"array","items":{"type":"array","items":{"type":"integer"},"minItems":2,"maxItems":2}}}))}}),
                ),
            ),
            (
                "note_drop",
                "Remove a note by id.",
                object(json!({"id":{"type":"integer"}})),
            ),
        ] {
            tools.push(FunctionTool {
                name: name.into(),
                description: description.into(),
                parameters,
                strict: true,
            });
        }
    }
    tools.push(FunctionTool {
        name: "finish".into(),
        description: "Finish the job with a valid report.".into(),
        parameters: object(json!({"report":report_schema})),
        strict: true,
    });
    tools
}

pub(crate) struct Capped {
    pub text: String,
    pub total: usize,
    pub shown: usize,
}

pub(crate) fn cap(text: &str, limit: usize) -> Capped {
    let total = text.chars().count();
    if total <= limit {
        return Capped {
            text: text.into(),
            total,
            shown: total,
        };
    }
    // Reserve marker space; a cap smaller than the marker still gets the full marker.
    let mut shown = limit.min(total);
    loop {
        let marker = format!("[cut: {shown} of {total} characters]");
        let next = limit.saturating_sub(marker.chars().count());
        if shown <= next || shown == 0 {
            let mut text: String = text.chars().take(shown).collect();
            text.push_str(&marker);
            let shown = text.chars().count();
            return Capped { text, total, shown };
        }
        shown = next;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cut_marker_counts_unicode_scalars_and_preserves_whole_characters() {
        let text = "界".repeat(200);
        let result = cap(&text, 60);
        assert_eq!(result.total, 200);
        let payload = result.text.split("[cut:").next().unwrap().chars().count();
        assert!(payload > 0 && payload < 60);
        assert_eq!(
            result.text,
            format!("{}[cut: {payload} of 200 characters]", "界".repeat(payload))
        );
        assert_eq!(result.shown, result.text.chars().count());
        assert!(result.shown <= 60);
        assert_eq!(cap("界a", 2).text, "界a");
        assert_eq!(cap(&text, 1).text, "[cut: 0 of 200 characters]");
    }
}

#[cfg(test)]
mod path_tests {
    use super::*;

    #[test]
    fn file_tools_refuse_unsafe_components_and_symlinks_but_read_safe_paths() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("root");
        fs::create_dir(&root).unwrap();
        fs::write(root.join("file"), "one\ntwo\nthree\n").unwrap();
        fs::write(directory.path().join("outside"), "outside contents").unwrap();
        let tools = Tools::new(&root).unwrap();
        let policy: Policy = serde_json::from_value(json!({"model":"test","reasoning_effort":null,"max_output_tokens":null,"max_steps":2,"read_max_lines":2,"reads_per_step":8,"tool_output_max_chars":1000,"search_max_hits":10,"outline":null})).unwrap();
        for path in [
            "",
            ".",
            "..",
            "../outside",
            "/outside",
            "a//b",
            "a/./b",
            "a/../b",
            "a/",
            "a\\b",
            "a\0b",
        ] {
            assert!(
                tools
                    .run(
                        "read",
                        &json!({"path":path,"start":1,"end":1}).to_string(),
                        &policy
                    )
                    .is_err(),
                "{path:?}"
            );
        }
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(directory.path().join("outside"), root.join("link"))
                .unwrap();
            assert!(
                tools
                    .run(
                        "read",
                        &json!({"path":"link","start":1,"end":1}).to_string(),
                        &policy
                    )
                    .is_err()
            );
        }
        assert_eq!(
            tools
                .run(
                    "read",
                    &json!({"path":"file","start":2,"end":100}).to_string(),
                    &policy
                )
                .unwrap(),
            "file: lines 2-3 of 3\n2| two\n3| three\n"
        );
        assert_eq!(
            tools
                .run(
                    "read",
                    &json!({"path":"file","start":1,"end":100}).to_string(),
                    &policy
                )
                .unwrap(),
            "file: lines 1-2 of 3\n1| one\n2| two\n[cut: read_max_lines 2; requested through line 3]\n"
        );
    }
}
