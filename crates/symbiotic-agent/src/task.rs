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
                    let name = name.as_str().ok_or_else(invalid)?;
                    if schema
                        .get("properties")
                        .and_then(Value::as_object)
                        .is_none_or(|p| !p.contains_key(name))
                    {
                        return Err(failure(&here, "required name is missing from properties"));
                    }
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
        "integer" => {
            value.is_i64() || value.is_u64() || value.as_f64().is_some_and(|n| n.fract() == 0.0)
        }
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
        (Value::Number(a), Value::Number(b)) => {
            a == b
                || (a.is_f64() && integer_float_equal(b, a.as_f64()))
                || (b.is_f64() && integer_float_equal(a, b.as_f64()))
        }
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

fn integer_float_equal(integer: &serde_json::Number, float: Option<f64>) -> bool {
    let Some(float) = float.filter(|f| f.fract() == 0.0) else {
        return false;
    };
    if let Some(number) = integer.as_u64() {
        float >= 0.0 && float < u64::MAX as f64 && float as u64 == number
    } else if let Some(number) = integer.as_i64() {
        float >= i64::MIN as f64 && float < i64::MAX as f64 && float as i64 == number
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn report_schema_checks_every_supported_constraint_and_json_path() {
        for (schema, good, bad, expected_path) in [
            (
                json!({"type":"string","minLength":2}),
                json!("界a"),
                json!("界"),
                "/",
            ),
            (
                json!({"type":["integer","null"]}),
                json!(null),
                json!(1.5),
                "/",
            ),
            (json!({"type":"number"}), json!(1.5), json!(true), "/"),
            (json!({"type":"boolean"}), json!(true), json!(1), "/"),
            (json!({"type":"null"}), json!(null), json!(false), "/"),
            (
                json!({"type":"array","minItems":1,"maxItems":2,"items":{"enum":[1,2]}}),
                json!([1, 2]),
                json!([3]),
                "/0",
            ),
            (json!({"minItems":1}), json!([1]), json!([]), "/"),
            (json!({"maxItems":1}), json!([1]), json!([1, 2]), "/"),
            (
                json!({"type":"object","properties":{"a/b~c":{"type":"string"}},"required":["a/b~c"],"additionalProperties":false}),
                json!({"a/b~c":"ok"}),
                json!({}),
                "/a~1b~0c",
            ),
            (
                json!({"properties":{},"additionalProperties":false}),
                json!({}),
                json!({"extra":1}),
                "/extra",
            ),
            (
                json!({"enum":[9007199254740992_u64]}),
                json!(9007199254740992_u64),
                json!(9007199254740993_u64),
                "/",
            ),
            (
                json!({"type":"integer","enum":[1]}),
                json!(1.0),
                json!(2),
                "/",
            ),
        ] {
            check_schema(&schema).unwrap();
            validate_report(&schema, &good).unwrap();
            assert!(
                validate_report(&schema, &bad)
                    .unwrap_err()
                    .starts_with(&format!("{expected_path}:")),
                "{schema} {bad}"
            );
        }
    }

    #[test]
    fn unsupported_or_malformed_schema_is_refused_at_any_depth() {
        for schema in [
            json!({"pattern":"x"}),
            json!({"properties":{"nested":{"format":"uri"}}}),
            json!({"items":{"type":"unknown"}}),
            json!({"type":["string","unknown"]}),
            json!({"type":[]}),
            json!({"additionalProperties":{}}),
            json!({"required":["missing"],"properties":{}}),
            json!({"minLength":-1}),
            json!({"minItems":"1"}),
            json!({"maxItems":1.5}),
            json!({"enum":[]}),
            json!({"description":1}),
            json!({"items":true}),
            json!({"properties":[]}),
        ] {
            assert!(check_schema(&schema).is_err(), "{schema}");
        }
    }
}
