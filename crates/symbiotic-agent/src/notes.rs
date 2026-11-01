use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::tools::safe_relative;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Reference {
    path: String,
    ranges: Vec<[usize; 2]>,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Kind {
    Covered,
    Finding,
    Question,
    Plan,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Note {
    kind: Kind,
    text: String,
    refs: Vec<Reference>,
}

#[derive(Serialize)]
struct Item {
    id: u64,
    step: usize,
    #[serde(flatten)]
    note: Note,
}

#[derive(Serialize)]
pub(crate) struct Notes {
    next_id: u64,
    items: Vec<Item>,
}

impl Notes {
    pub fn new() -> Self {
        Self {
            next_id: 1,
            items: Vec::new(),
        }
    }

    pub fn add(&mut self, note: Note, step: usize) -> Result<u64, String> {
        for reference in &note.refs {
            safe_relative(&reference.path)?;
            for [start, end] in &reference.ranges {
                if *start < 1 || start > end {
                    return Err("note refs require 1 <= start <= end".into());
                }
            }
        }
        let id = self.next_id;
        self.next_id = self.next_id.checked_add(1).ok_or("note ids exhausted")?;
        self.items.push(Item { id, step, note });
        Ok(id)
    }

    pub fn drop(&mut self, id: u64) -> Result<(), String> {
        let index = self
            .items
            .iter()
            .position(|item| item.id == id)
            .ok_or_else(|| format!("unknown note id: {id}"))?;
        self.items.remove(index);
        Ok(())
    }

    pub fn compact(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    pub fn write(&self, out: &Path) -> Result<(), String> {
        let temporary = out.join(format!(".notes-{}.tmp", Uuid::new_v4()));
        let result = (|| {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)?;
            serde_json::to_writer(&mut file, self)?;
            file.write_all(b"\n")?;
            drop(file);
            fs::rename(&temporary, out.join("notes.json"))
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result.map_err(|e: std::io::Error| format!("notes.json: {e}"))
    }
}
