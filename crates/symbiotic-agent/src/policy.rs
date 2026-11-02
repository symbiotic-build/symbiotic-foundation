use serde::{Deserialize, Deserializer};

// Unlike serde's default Option handling, nullable fields must still be present.
pub(crate) fn present<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Policy {
    pub model: String,
    #[serde(deserialize_with = "present")]
    pub reasoning_effort: Option<String>,
    #[serde(deserialize_with = "present")]
    pub max_output_tokens: Option<u32>,
    pub max_steps: usize,
    pub read_max_lines: usize,
    pub reads_per_step: usize,
    pub tool_output_max_chars: usize,
    pub search_max_hits: usize,
    #[serde(default = "one")]
    pub keep_results_steps: usize,
    #[serde(deserialize_with = "present")]
    pub outline: Option<String>,
}

fn one() -> usize {
    1
}

impl Policy {
    pub fn validate(&self) -> Result<(), String> {
        if self.max_steps < 2 {
            return Err("max_steps must be at least 2".into());
        }
        for (name, value) in [
            ("read_max_lines", self.read_max_lines),
            ("reads_per_step", self.reads_per_step),
            ("tool_output_max_chars", self.tool_output_max_chars),
            ("search_max_hits", self.search_max_hits),
            ("keep_results_steps", self.keep_results_steps),
        ] {
            if value == 0 {
                return Err(format!("{name} must be positive"));
            }
        }
        if self.max_output_tokens == Some(0) {
            return Err("max_output_tokens must be positive or null".into());
        }
        Ok(())
    }
}
