use serde::Deserialize;
use serde_json::Value;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Task {
    pub instructions: String,
    #[serde(deserialize_with = "crate::policy::present")]
    pub task: Value,
    pub report_schema: Value,
}

fn path(parent: &str, key: &str) -> String {
    format!("{parent}/{}", key.replace('~', "~0").replace('/', "~1"))
}

fn failure(at: &str, reason: &str) -> String {
    format!("{}: {reason}", if at.is_empty() { "/" } else { at })
}

fn known_type(name: &str) -> bool {
    matches!(
        name,
        "object" | "array" | "string" | "integer" | "number" | "boolean" | "null"
    )
}

pub(crate) fn check_schema(schema: &Value) -> Result<(), String> {
    schema_at(schema, "")
}

fn schema_at(schema: &Value, at: &str) -> Result<(), String> {
    let object = schema
        .as_object()
        .ok_or_else(|| failure(at, "schema must be an object"))?;
    for (key, value) in object {
        let here = path(at, key);
        let invalid = || failure(&here, "invalid schema keyword value");
        match key.as_str() {
            "type" => {
                let valid = match value {
                    Value::String(name) => known_type(name),
                    Value::Array(names) => {
                        !names.is_empty()
                            && names.iter().all(|v| v.as_str().is_some_and(known_type))
                    }
                    _ => false,
                };
                if !valid {
                    return Err(invalid());
                }
            }
            "properties" => {
                let properties = value.as_object().ok_or_else(invalid)?;
                for (name, child) in properties {
                    schema_at(child, &path(&here, name))?;
                }
            }
            "required" => {
                let names = value.as_array().ok_or_else(invalid)?;
                for name in names {
                    name.as_str().ok_or_else(invalid)?;
                }
            }
            "additionalProperties" if value.is_boolean() => {}
            "items" => schema_at(value, &here)?,
            "enum" if value.as_array().is_some_and(|v| !v.is_empty()) => {}
            "minItems" | "maxItems" | "minLength" if value.as_u64().is_some() => {}
            "description" if value.is_string() => {}
            "additionalProperties"
            | "enum"
            | "minItems"
            | "maxItems"
            | "minLength"
            | "description" => return Err(invalid()),
            _ => return Err(failure(&here, "unsupported schema keyword")),
        }
    }
    Ok(())
}

fn matches_type(value: &Value, name: &str) -> bool {
    match name {
        "object" => value.is_object(),
        "array" => value.is_array(),
        "string" => value.is_string(),
        "integer" => value
            .as_number()
            .and_then(normalized_decimal)
            .is_some_and(|(_, digits, exponent)| digits.is_empty() || exponent >= 0),
        "number" => value.is_number(),
        "boolean" => value.is_boolean(),
        "null" => value.is_null(),
        _ => false,
    }
}

pub(crate) fn validate_report(schema: &Value, report: &Value) -> Result<(), String> {
    validate_at(schema, report, "")
}

fn validate_at(schema: &Value, value: &Value, at: &str) -> Result<(), String> {
    if let Some(types) = schema.get("type") {
        let valid = match types {
            Value::String(name) => matches_type(value, name),
            Value::Array(names) => names
                .iter()
                .any(|v| v.as_str().is_some_and(|n| matches_type(value, n))),
            _ => false,
        };
        if !valid {
            return Err(failure(at, &format!("expected type {types}")));
        }
    }
    if let Some(choices) = schema.get("enum").and_then(Value::as_array)
        && !choices.iter().any(|choice| json_equal(choice, value))
    {
        return Err(failure(at, "value is not in enum"));
    }
    if let Some(object) = value.as_object() {
        if let Some(required) = schema.get("required").and_then(Value::as_array) {
            for name in required.iter().filter_map(Value::as_str) {
                if !object.contains_key(name) {
                    return Err(failure(&path(at, name), "required property is missing"));
                }
            }
        }
        for (name, child) in object {
            if let Some(child_schema) = schema.get("properties").and_then(|p| p.get(name)) {
                validate_at(child_schema, child, &path(at, name))?;
            } else if schema.get("additionalProperties") == Some(&Value::Bool(false)) {
                return Err(failure(
                    &path(at, name),
                    "additional property is not allowed",
                ));
            }
        }
    }
    if let Some(array) = value.as_array() {
        for (keyword, failed) in [
            (
                "minItems",
                schema["minItems"]
                    .as_u64()
                    .is_some_and(|n| (array.len() as u64) < n),
            ),
            (
                "maxItems",
                schema["maxItems"]
                    .as_u64()
                    .is_some_and(|n| (array.len() as u64) > n),
            ),
        ] {
            if failed {
                return Err(failure(at, &format!("{keyword} check failed")));
            }
        }
        if let Some(items) = schema.get("items") {
            for (index, child) in array.iter().enumerate() {
                validate_at(items, child, &path(at, &index.to_string()))?;
            }
        }
    }
    if let Some(text) = value.as_str()
        && schema["minLength"]
            .as_u64()
            .is_some_and(|n| (text.chars().count() as u64) < n)
    {
        return Err(failure(at, "minLength check failed"));
    }
    Ok(())
}

// JSON Schema compares numeric values, not serde_json's integer/float representations.
fn json_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Number(a), Value::Number(b)) => normalized_decimal(a)
            .zip(normalized_decimal(b))
            .is_some_and(|(a, b)| a == b),
        (Value::Array(a), Value::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(a, b)| json_equal(a, b))
        }
        (Value::Object(a), Value::Object(b)) => {
            a.len() == b.len()
                && a.iter()
                    .all(|(k, v)| b.get(k).is_some_and(|w| json_equal(v, w)))
        }
        _ => left == right,
    }
}

// Number supplies valid JSON number text. Empty digits represent canonical unsigned zero.
fn normalized_decimal(number: &serde_json::Number) -> Option<(bool, String, i64)> {
    let text = number.to_string();
    let negative = text.starts_with('-');
    let unsigned = text.strip_prefix('-').unwrap_or(&text);
    let (mantissa, exponent) = match unsigned.split_once(['e', 'E']) {
        Some((mantissa, exponent)) => (mantissa, exponent.parse::<i64>().ok()?),
        None => (unsigned, 0),
    };
    let fraction_len = mantissa
        .split_once('.')
        .map_or(0, |(_, fraction)| fraction.len());
    let digits = mantissa.replace('.', "");
    let significant = digits.trim_start_matches('0');
    let trimmed = significant.trim_end_matches('0');
    if trimmed.is_empty() {
        return Some((false, String::new(), 0));
    }
    let trailing_zeros = i64::try_from(significant.len() - trimmed.len()).ok()?;
    let adjustment = trailing_zeros.checked_sub(i64::try_from(fraction_len).ok()?)?;
    Some((
        negative,
        trimmed.to_owned(),
        exponent.checked_add(adjustment)?,
    ))
}
