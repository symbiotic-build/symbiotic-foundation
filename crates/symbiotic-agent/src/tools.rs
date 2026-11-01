use std::{
    collections::HashSet,
    fs,
    io::ErrorKind,
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
            let bytes = match fs::read(path) {
                Ok(bytes) => bytes,
                Err(error) if error.kind() == ErrorKind::PermissionDenied => return Ok(()),
                Err(error) => return Err(error.to_string()),
            };
            if bytes[..bytes.len().min(8192)].contains(&0) {
                return Ok(());
            }
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

    fn policy() -> Policy {
        serde_json::from_value(json!({"model":"test","reasoning_effort":null,"max_output_tokens":null,"max_steps":2,"read_max_lines":2,"reads_per_step":8,"tool_output_max_chars":1000,"search_max_hits":10,"outline":null})).unwrap()
    }

    #[test]
    fn list_with_null_path_returns_sorted_root_entries() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("z.txt"), "last").unwrap();
        fs::create_dir(root.path().join("a")).unwrap();
        fs::write(root.path().join("b.txt"), "middle").unwrap();
        let tools = Tools::new(root.path()).unwrap();
        assert_eq!(
            tools.run("list", r#"{"path":null}"#, &policy()).unwrap(),
            "a/\nb.txt\nz.txt"
        );
    }

    #[test]
    fn search_skips_non_utf8_and_special_files_and_returns_text_hits() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("a-invalid.txt"), b"needle\xff").unwrap();
        #[cfg(unix)]
        {
            assert!(
                std::process::Command::new("mkfifo")
                    .arg(root.path().join("b-fifo"))
                    .status()
                    .unwrap()
                    .success()
            );
            std::os::unix::fs::symlink("missing", root.path().join("d-dangling")).unwrap();
        }
        fs::write(root.path().join("z-text.txt"), "needle text\n").unwrap();
        let tools = Tools::new(root.path()).unwrap();
        assert_eq!(
            tools
                .run(
                    "search",
                    r#"{"pattern":"needle","path":null,"max_hits":null}"#,
                    &policy()
                )
                .unwrap(),
            "z-text.txt:1: needle text"
        );
    }

    #[cfg(unix)]
    #[test]
    fn search_skips_unreadable_files_and_directories_and_returns_text_hits() {
        use std::os::unix::fs::PermissionsExt;

        let root = tempfile::tempdir().unwrap();
        let file = root.path().join("a-unreadable.txt");
        let directory = root.path().join("b-unreadable");
        fs::write(&file, "needle unreadable").unwrap();
        fs::create_dir(&directory).unwrap();
        fs::write(directory.join("hidden.txt"), "needle hidden").unwrap();
        fs::write(root.path().join("z-text.txt"), "needle text\n").unwrap();
        let file_permissions = fs::metadata(&file).unwrap().permissions();
        let directory_permissions = fs::metadata(&directory).unwrap().permissions();
        fs::set_permissions(&file, fs::Permissions::from_mode(0o0)).unwrap();
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o0)).unwrap();
        let tools = Tools::new(root.path()).unwrap();
        let result = tools.run(
            "search",
            r#"{"pattern":"needle","path":null,"max_hits":null}"#,
            &policy(),
        );
        fs::set_permissions(&file, file_permissions).unwrap();
        fs::set_permissions(&directory, directory_permissions).unwrap();
        assert_eq!(result.unwrap(), "z-text.txt:1: needle text");
    }

    #[test]
    fn search_refuses_missing_or_outside_starting_paths() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("root");
        fs::create_dir(&root).unwrap();
        fs::write(directory.path().join("outside"), "needle outside").unwrap();
        let tools = Tools::new(&root).unwrap();
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(directory.path().join("outside"), root.join("link"))
                .unwrap();
        }
        for path in ["missing", "../outside", "link"] {
            assert!(
                tools
                    .run(
                        "search",
                        &json!({"pattern":"needle","path":path,"max_hits":null}).to_string(),
                        &policy()
                    )
                    .is_err(),
                "{path}"
            );
        }
    }

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
