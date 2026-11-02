use std::{
    collections::HashSet,
    fs,
    io::{BufRead, BufReader, ErrorKind, Read as _},
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
    #[serde(deserialize_with = "crate::policy::present")]
    path: Option<String>,
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
        let file =
            fs::File::open(self.resolve(&args.path)?).map_err(|e| format!("{}: {e}", args.path))?;
        let requested_end = args.end.min(args.start.saturating_add(max_lines - 1));
        let mut selected = String::new();
        let mut line_count = 0;
        for line in BufReader::new(file).lines() {
            let line = line.map_err(|e| format!("{}: {e}", args.path))?;
            line_count += 1;
            if (args.start..=requested_end).contains(&line_count) {
                selected.push_str(&format!("{line_count}| {line}\n"));
            }
        }
        let end = args.end.min(line_count);
        if args.start > end {
            return Err("read start is beyond the last line".into());
        }
        let shown_end = requested_end.min(line_count);
        let mut result = format!(
            "{}: lines {}-{} of {}\n{selected}",
            args.path, args.start, shown_end, line_count
        );
        if shown_end < end {
            result.push_str(&format!(
                "[cut: read_max_lines {max_lines}; requested through line {end}]\n"
            ));
        }
        Ok(result)
    }

    fn list(&self, args: List) -> Result<String, String> {
        let path = match args.path {
            Some(path) => self.resolve(&path)?,
            None => self.root.clone(),
        };
        let mut names = Vec::new();
        for entry in fs::read_dir(path).map_err(|e| e.to_string())? {
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
        let metadata = match fs::metadata(path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == ErrorKind::PermissionDenied => return Ok(()),
            Err(error) => return Err(error.to_string()),
        };
        if metadata.is_dir() {
            if path.file_name().is_some_and(|name| name == ".git")
                || relative.rsplit('/').next() == Some(".git")
            {
                return Ok(());
            }
            let entries = match fs::read_dir(path) {
                Ok(entries) => entries,
                Err(error) if error.kind() == ErrorKind::PermissionDenied => return Ok(()),
                Err(error) => return Err(error.to_string()),
            };
            let canonical = fs::canonicalize(path).map_err(|e| e.to_string())?;
            if !ancestors.insert(canonical.clone()) {
                return Ok(());
            }
            let mut entries = entries
                .filter_map(|entry| match entry {
                    Err(error) if error.kind() == ErrorKind::PermissionDenied => None,
                    entry => Some(entry),
                })
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
                let resolved = match fs::canonicalize(entry.path()) {
                    Ok(resolved) => resolved,
                    Err(error)
                        if matches!(
                            error.kind(),
                            ErrorKind::PermissionDenied | ErrorKind::NotFound
                        ) =>
                    {
                        continue;
                    }
                    Err(error) => return Err(error.to_string()),
                };
                if !resolved.starts_with(&self.root) {
                    continue;
                }
                self.walk(&resolved, &child_relative, pattern, limit, hits, ancestors)?;
                if hits.len() == limit {
                    break;
                }
            }
            ancestors.remove(&canonical);
        } else if metadata.is_file() {
            let mut file = match fs::File::open(path) {
                Ok(file) => file,
                Err(error) if error.kind() == ErrorKind::PermissionDenied => return Ok(()),
                Err(error) => return Err(error.to_string()),
            };
            let mut bytes = Vec::new();
            file.by_ref()
                .take(8192)
                .read_to_end(&mut bytes)
                .map_err(|e| e.to_string())?;
            if bytes.contains(&0) {
                return Ok(());
            }
            file.read_to_end(&mut bytes).map_err(|e| e.to_string())?;
            let Ok(text) = std::str::from_utf8(&bytes) else {
                return Ok(());
            };
            for (index, line) in text.lines().enumerate() {
                if pattern.is_match(line) {
                    hits.push(format!("{relative}:{}: {line}", index + 1));
                    if hits.len() == limit {
                        break;
                    }
                }
            }
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
                "List sorted directory entries; null path lists the root.",
                object(json!({"path":{"type":["string","null"]}})),
            ),
            (
                "note",
                "Add a note with syntactically checked references.",
                object(
                    json!({"kind":{"type":"string","enum":["covered","finding","question","plan"]},"text":{"type":"string"},"refs":{"type":"array","items":object(json!({"path":{"type":"string"},"ranges":{"type":"array","items":{"type":"array","items":{"type":"integer"}}}}))}}),
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
        strict: false,
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
    fn request_tools_use_strict_schemas_except_finish() {
        use symbiotic_model::responses::{ResponsesRequest, ToolChoice};

        let schema = json!({"type":"object","properties":{"items":{"type":"array","items":{"type":"string"},"minItems":1}},"required":["items"]});
        for final_step in [false, true] {
            let request = ResponsesRequest {
                model: "test".into(),
                instructions: "Return a report.".into(),
                input: Vec::new(),
                tools: definitions(&schema, final_step),
                tool_choice: if final_step {
                    ToolChoice::Required
                } else {
                    ToolChoice::Auto
                },
                parallel_tool_calls: true,
                reasoning_effort: None,
                max_output_tokens: None,
            };
            let body: Value = serde_json::from_slice(&request.encode().unwrap()).unwrap();
            let flags: Vec<_> = body["tools"]
                .as_array()
                .unwrap()
                .iter()
                .map(|tool| json!({"name":tool["name"],"strict":tool["strict"]}))
                .collect();
            let expected = if final_step {
                json!([{"name":"finish","strict":false}])
            } else {
                json!([
                    {"name":"read","strict":true},
                    {"name":"search","strict":true},
                    {"name":"list","strict":true},
                    {"name":"note","strict":true},
                    {"name":"note_drop","strict":true},
                    {"name":"finish","strict":false}
                ])
            };
            assert_eq!(json!(flags), expected);
        }
    }
}
