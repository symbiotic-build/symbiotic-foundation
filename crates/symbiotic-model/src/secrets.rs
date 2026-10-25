//! One non-Debug, non-serializable zeroizing container for provider secrets.
use base64::{Engine, engine::general_purpose};
use std::ops::{Deref, DerefMut};
use zeroize::{Zeroize, Zeroizing};

/// Owns a secret and zeroizes it on drop, including clones and derived buffers.
/// Ordinary diagnostics and serialization cannot expose it.
/// ```compile_fail
/// use symbiotic_model::SecretValue;
/// let secret = SecretValue::new(String::from("synthetic"));
/// println!("{secret:?}");
/// ```
#[derive(Clone)]
pub struct SecretValue<T: Zeroize>(Zeroizing<T>);
impl<T: Zeroize> SecretValue<T> {
    /// Take ownership of a value and zeroize its storage when it is dropped.
    pub fn new(value: T) -> Self {
        Self(Zeroizing::new(value))
    }
}
impl<T: Zeroize> Deref for SecretValue<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}
impl<T: Zeroize> DerefMut for SecretValue<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.0
    }
}
impl From<String> for SecretValue<String> {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}
impl From<&str> for SecretValue<String> {
    fn from(value: &str) -> Self {
        Self::new(value.to_owned())
    }
}
impl From<&String> for SecretValue<String> {
    fn from(value: &String) -> Self {
        Self::new(value.clone())
    }
}

/// Opaque Foundation-owned credential guard for adapter composition.
/// It can be forwarded or cloned, but exposes neither the secret nor a policy
/// override. Only Foundation constructs it; every result uses the same boundary.
/// ```compile_fail
/// use symbiotic_model::{OpenAiCompatibleChatProvider, ModelProvider};
/// let provider = OpenAiCompatibleChatProvider::new("op", "model", "http://localhost", "key");
/// println!("{:?}", provider.credential_boundary().unwrap());
/// ```
/// ```compile_fail
/// use symbiotic_model::{OpenAiCompatibleChatProvider, ModelProvider};
/// let provider = OpenAiCompatibleChatProvider::new("op", "model", "http://localhost", "key");
/// println!("{}", provider.credential_boundary().unwrap().secret());
/// ```
#[derive(Clone)]
pub struct CredentialBoundary {
    key: SecretValue<String>,
}

impl CredentialBoundary {
    pub(crate) fn new(key: SecretValue<String>) -> Self {
        Self { key }
    }

    pub(crate) fn secret(&self) -> &str {
        self.key.as_str()
    }
}

/// The finite credential encoding set.
pub fn credential_encodings(secret: &str) -> Result<SecretValue<Vec<String>>, crate::ModelError> {
    let mut encodings = SecretValue::new(vec![secret.to_owned()]);
    let escaped = SecretValue::new(serde_json::to_string(secret).map_err(|_| {
        crate::ModelError::Provider(symbiotic_core::DiagnosticCode::InvalidCredentialEncoding)
    })?);
    encodings.push(escaped[1..escaped.len() - 1].to_owned());
    for all in [false, true] {
        for upper in [false, true] {
            let mut encoded = SecretValue::new(String::new());
            for byte in secret.bytes() {
                if !all && (byte.is_ascii_alphanumeric() || b"-._~".contains(&byte)) {
                    encoded.push(char::from(byte));
                } else {
                    let digits = if upper {
                        b"0123456789ABCDEF"
                    } else {
                        b"0123456789abcdef"
                    };
                    encoded.push('%');
                    encoded.push(char::from(digits[usize::from(byte >> 4)]));
                    encoded.push(char::from(digits[usize::from(byte & 15)]));
                }
            }
            encodings.push(std::mem::take(&mut *encoded));
        }
    }
    for engine in [
        general_purpose::STANDARD,
        general_purpose::STANDARD_NO_PAD,
        general_purpose::URL_SAFE,
        general_purpose::URL_SAFE_NO_PAD,
    ] {
        encodings.push(engine.encode(secret.as_bytes()));
    }
    Ok(encodings)
}

/// Refuse credential echoes in decoded strings, object keys and encoded numbers.
/// Used only by the complete adapter-result boundary.
pub(crate) fn check_response(
    value: &serde_json::Value,
    secret: &str,
) -> Result<(), crate::ModelError> {
    fn contains_text(text: &str, encodings: &[String]) -> bool {
        encodings.iter().any(|secret| text.contains(secret))
    }
    fn contains(value: &serde_json::Value, encodings: &[String]) -> bool {
        match value {
            serde_json::Value::String(text) => contains_text(text, encodings),
            serde_json::Value::Array(values) => {
                values.iter().any(|value| contains(value, encodings))
            }
            serde_json::Value::Object(values) => values
                .iter()
                .any(|(key, value)| contains_text(key, encodings) || contains(value, encodings)),
            serde_json::Value::Number(number) => {
                contains_text(&SecretValue::new(number.to_string()), encodings)
            }
            _ => false,
        }
    }
    if secret.is_empty() {
        return Ok(());
    }
    let encodings = credential_encodings(secret)?;
    let wire = SecretValue::new(serde_json::to_string(value).map_err(|_| {
        crate::ModelError::Provider(symbiotic_core::DiagnosticCode::InvalidProviderResponse)
    })?);
    if contains(value, &encodings) || contains_text(&wire, &encodings) {
        return Err(crate::ModelError::Provider(
            symbiotic_core::DiagnosticCode::CredentialBearingProviderResponseRefused,
        ));
    }
    Ok(())
}

/// Type erasure for dynamic/composed adapters. Policy is still enforced only
/// by `credential_boundary` using the credential owner's opaque guard.
pub(crate) fn composed_result<T: serde::Serialize + serde::de::DeserializeOwned>(
    provider: &dyn crate::ModelProvider,
    result: Result<T, crate::ModelError>,
) -> Result<T, crate::ModelError> {
    let Some(boundary) = provider.credential_boundary() else {
        if provider.credential_fingerprint().is_some()
            || !matches!(
                provider.descriptor().auth_mode,
                crate::ProviderAuthMode::None
            )
        {
            return Err(crate::ModelError::Provider(
                symbiotic_core::DiagnosticCode::CredentialResultBoundaryIsUnavailable,
            ));
        }
        return result;
    };
    let result = result.and_then(|response| {
        serde_json::to_value(response).map_err(|_| {
            crate::ModelError::Provider(symbiotic_core::DiagnosticCode::InvalidAdapterResponse)
        })
    });
    let value = credential_boundary(result, boundary)?;
    // Decode only Foundation's own typed serialization after the boundary.
    serde_json::from_value(value).map_err(|_| {
        crate::ModelError::Provider(symbiotic_core::DiagnosticCode::InvalidAdapterResponse)
    })
}

/// Adapter responses support raw disposal and runtime finish-label normalization.
pub(crate) trait CredentialResponse: serde::Serialize {
    fn discard_raw(&mut self);
    #[cfg(feature = "queue")]
    fn normalize_finish_reason(&mut self) {}
}

// The erased representation used by object-safe composed adapters. It follows
// the same inspection and raw-disposal path as concrete responses.
impl CredentialResponse for serde_json::Value {
    fn discard_raw(&mut self) {
        if let Some(object) = self.as_object_mut() {
            object.remove("raw_provider_response");
        }
    }
}

macro_rules! credential_response {
    ($($response:ty),+ $(,)?) => {$(
        impl CredentialResponse for $response {
            fn discard_raw(&mut self) {
                self.raw_provider_response = None;
            }
        }
    )+};
}
impl CredentialResponse for crate::ChatResponse {
    fn discard_raw(&mut self) {
        self.raw_provider_response = None;
    }
    #[cfg(feature = "queue")]
    fn normalize_finish_reason(&mut self) {
        self.finish_reason =
            crate::provider_finish_reason(&mut self.trace, self.finish_reason.take());
    }
}

credential_response!(
    crate::EmbeddingResponse,
    crate::RerankResponse,
    crate::ClassifyResponse
);

/// The sole credential boundary for a complete adapter result, after HTTP,
/// typed decoding and answer validation, and before runtime bookkeeping.
/// Inspect raw JSON and the final typed value together; never export raw JSON
/// or provider-controlled error text from a credential-bearing call.
/// Keyless direct calls retain their explicit raw response return value.
pub(crate) fn credential_boundary<T: CredentialResponse>(
    result: Result<T, crate::ModelError>,
    boundary: &CredentialBoundary,
) -> Result<T, crate::ModelError> {
    let secret = boundary.secret();
    if secret.is_empty() {
        return result;
    }
    let mut response = match result {
        Ok(response) => response,
        Err(error) => {
            if let Some((status, retry)) = error.http_details() {
                check_response(
                    &serde_json::json!({"status":status,"retry_after_seconds":retry}),
                    secret,
                )?;
            }
            return Err(error);
        }
    };
    let value = serde_json::to_value(&response).map_err(|_| {
        crate::ModelError::Provider(symbiotic_core::DiagnosticCode::InvalidResponse)
    })?;
    check_response(&value, secret)?;
    response.discard_raw();
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn regression_keyless_credential_boundary_preserves_raw_results() {
        let boundary = CredentialBoundary::new(String::new().into());
        let response = credential_boundary(
            Ok(serde_json::json!({"text":"OK", "raw_provider_response":{
                "choices":[{"message":{"reasoning_content":"PRIVATE_REASONING"}}]
            }})),
            &boundary,
        )
        .unwrap();
        assert_eq!(response["text"], "OK");
        assert_eq!(
            response["raw_provider_response"]["choices"][0]["message"]["reasoning_content"],
            "PRIVATE_REASONING"
        );
        assert!(matches!(
            credential_boundary::<serde_json::Value>(
                Err(crate::ModelError::Timeout(
                    symbiotic_core::DiagnosticCode::HttpTimeout
                )),
                &boundary,
            ),
            Err(crate::ModelError::Timeout(
                symbiotic_core::DiagnosticCode::HttpTimeout
            ))
        ));
    }

    #[test]
    fn rejects_every_declared_credential_encoding() {
        let secret = "key-\"/\n+?=é";
        let encodings = credential_encodings(secret).unwrap();
        assert!(encodings.len() >= 10);
        for encoded in encodings.iter() {
            let value = serde_json::json!({"content": format!("prefix {encoded} suffix")});
            assert!(check_response(&value, secret).is_err(), "{encoded}");
        }
        assert!(check_response(&serde_json::json!("ordinary provider answer"), secret).is_ok());
    }
    #[test]
    fn credential_echoes_are_refused_in_nested_fields_keys_and_numbers() {
        for value in [
            serde_json::json!({"vectors": [[123456789]]}),
            serde_json::json!({"nested": ["prefix123456789suffix"]}),
            serde_json::json!({"123456789": "safe"}),
        ] {
            assert!(check_response(&value, "123456789").is_err());
            assert!(check_response(&value, "").is_ok());
        }
    }
    #[test]
    fn numeric_screen_uses_literal_text_only() {
        for text in ["123400000", "1.234e+8", "0.123456789", "-123400000"] {
            let value: serde_json::Value = serde_json::from_str(text).unwrap();
            assert!(check_response(&value, text).is_err());
        }
        let value: serde_json::Value = serde_json::from_str("1.234e8").unwrap();
        assert!(check_response(&value, "123400000").is_ok());
        let value: serde_json::Value = serde_json::from_str("123400000").unwrap();
        assert!(check_response(&value, "1.234e8").is_ok());
    }

    #[test]
    fn regression_numeric_error_hints_cannot_echo_credentials() {
        for wrapped in [false, true] {
            let error = crate::ModelError::Http {
                primary: Box::new(crate::ModelError::RateLimited(
                    symbiotic_core::DiagnosticCode::HttpRateLimited,
                )),
                status: 429,
                retry_after_seconds: Some(123456789),
            };
            let error = if wrapped {
                crate::ModelError::Diagnostics {
                    primary: Box::new(error),
                    secondary: vec![symbiotic_core::DiagnosticCode::StorageFailure],
                }
            } else {
                error
            };
            let boundary = CredentialBoundary::new("123456789".into());
            let result = credential_boundary::<serde_json::Value>(Err(error), &boundary);
            assert!(matches!(
                result,
                Err(crate::ModelError::Provider(
                    symbiotic_core::DiagnosticCode::CredentialBearingProviderResponseRefused
                ))
            ));
        }
    }

    #[test]
    fn secret_clones_are_owned_zeroizing_containers() {
        let mut secret = SecretValue::new(String::from("synthetic"));
        let clone = secret.clone();
        secret.zeroize();
        assert!(secret.is_empty());
        assert_eq!(clone.as_str(), "synthetic");
    }
}
