#![cfg(unix)]
//! Public store and import boundaries; all tokens are synthetic.
use base64::Engine;
use serde_json::{Value, json};
use std::{
    fs,
    os::unix::fs::{PermissionsExt, symlink},
    path::Path,
    process::Command,
};
use symbiotic_credential_process::{
    login::{Login, LoginHealth, import_native},
    protect_process,
    secrets::{LoginFamily, SecretSource},
};

const LIMIT: usize = 1024 * 1024;
const CLIENT: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}
fn write(path: &Path, value: &Value) {
    fs::write(path, serde_json::to_vec(value).unwrap()).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
}
fn native(path: &Path) -> String {
    let access = format!(
        "synthetic.{}.signature",
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(&json!({"exp": now()+3600})).unwrap())
    );
    write(
        path,
        &json!({"tokens":{"access_token":access,"refresh_token":"synthetic-refresh","id_token":"synthetic-id","account_id":"synthetic-account"}}),
    );
    access
}
fn store_value() -> Value {
    json!({"version":1,"family":"chatgpt_codex","issuer":"https://auth.openai.com","client_id":CLIENT,"access_token":"synthetic-access","refresh_token":"synthetic-refresh","id_token":null,"account_id":"synthetic-account","principal_type":null,"principal_id":null,"expires_at":now()+3600})
}
fn open(path: &Path) -> Result<Login, symbiotic_egress::EgressError> {
    Login::open(path, LoginFamily::ChatgptCodex, LIMIT)
}

#[test]
fn login_source_uses_the_design_encoding_and_typed_routes_refuse_it() {
    let value = json!({"backend":"login_file","path":"login.json","family":"chatgpt_codex"});
    let source: SecretSource = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(serde_json::to_value(source).unwrap(), value);
    // Route fields are the public version-4 config format, with valid defaults.
    let mut route = json!({
        "tenant":"tenant", "account":"account", "account_sharing_key":null,
        "max_attempts":3, "route":"chat", "secret_ref":"login", "secret":{"backend":"none"},
        "destination":"https://example.com/v1", "model":"test-model",
        "provider":{"kind":"open_ai_chat","operator":"test"},
        "allow_loopback_http":false, "max_field_bytes":1024, "max_output_tokens":100,
        "max_in_flight":4, "requests_per_minute":null, "input_units_per_minute":null,
        "timeout_seconds":1
    });
    route["secret"] = value;
    let route = serde_json::from_value(route).unwrap();
    assert!(matches!(
        symbiotic_credential_process::validate_routes(&[route], 8 * 1024 * 1024),
        Err(symbiotic_egress::EgressError::InvalidRequest)
    ));
}

#[test]
fn unsafe_or_mismatched_stores_are_refused() {
    protect_process().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    for (field, value) in [
        ("version", json!(2)),
        ("family", json!("xai_grok")),
        ("issuer", json!("https://elsewhere.invalid")),
        ("client_id", json!("wrong-client")),
        ("unknown", json!(true)),
        ("access_token", json!("bad\r\nheader")),
        ("refresh_token", json!("")),
        ("account_id", Value::Null),
    ] {
        let mut record = store_value();
        record[field] = value;
        write(&path, &record);
        assert!(open(&path).is_err(), "accepted {field}");
    }
    write(&path, &store_value());
    fs::set_permissions(&path, fs::Permissions::from_mode(0o640)).unwrap();
    assert!(open(&path).is_err(), "accepted group-readable store");
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    let link = dir.path().join("link");
    symlink(&path, &link).unwrap();
    assert!(open(&link).is_err(), "accepted symlink");
    assert!(
        Login::open(&path, LoginFamily::ChatgptCodex, 8).is_err(),
        "accepted oversized store"
    );
    assert!(open(&path).is_ok(), "valid store refused");
}

#[test]
fn wrong_owner_store_is_refused_when_chown_is_available() {
    protect_process().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    write(&path, &store_value());
    let name =
        std::ffi::CString::new(std::os::unix::ffi::OsStrExt::as_bytes(path.as_os_str())).unwrap();
    // SAFETY: the path is a live nul-terminated string; this changes only our fixture.
    let owner = unsafe { libc::geteuid() };
    // A root test process can exercise a foreign owner. Other users cannot chown.
    if owner != 0 {
        eprintln!("wrong-owner case unavailable: test UID cannot chown to a foreign UID");
        return;
    }
    // SAFETY: chown reads the fixture pathname and retains no pointers.
    assert_eq!(unsafe { libc::chown(name.as_ptr(), 1, u32::MAX) }, 0);
    assert!(open(&path).is_err());
}

#[tokio::test]
async fn import_obeys_lifetime_lock_and_explicit_replacement() {
    protect_process().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("auth.json");
    let path = dir.path().join("store");
    let access = native(&source);
    import_native(LoginFamily::ChatgptCodex, &source, &path, false, LIMIT).unwrap();
    let login = open(&path).unwrap();
    assert!(
        import_native(LoginFamily::ChatgptCodex, &source, &path, true, LIMIT).is_err(),
        "import acquired a held lock"
    );
    assert!(open(&path).is_err(), "second owner acquired lock");
    assert_eq!(
        login.request().await.unwrap().headers()["authorization"],
        format!("Bearer {access}")
    );
    drop(login);
    assert!(
        import_native(LoginFamily::ChatgptCodex, &source, &path, false, LIMIT).is_err(),
        "import overwrote without replace"
    );
    import_native(LoginFamily::ChatgptCodex, &source, &path, true, LIMIT).unwrap();
    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let stored: Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    assert_eq!(
        stored,
        json!({"version":1,"family":"chatgpt_codex","issuer":"https://auth.openai.com","client_id":CLIENT,"access_token":access,"refresh_token":"synthetic-refresh","id_token":"synthetic-id","account_id":"synthetic-account","principal_type":null,"principal_id":null,"expires_at":stored["expires_at"]})
    );
}

#[tokio::test]
async fn set_fence_requires_import_before_any_request() {
    protect_process().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let source = dir.path().join("auth.json");
    write(&path, &store_value());
    native(&source);
    fs::write(dir.path().join("store.renewal"), [1]).unwrap();
    fs::set_permissions(
        dir.path().join("store.renewal"),
        fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    let login = open(&path).unwrap();
    assert_eq!(login.health().await.unwrap().0, LoginHealth::LoginRequired);
    assert!(login.request().await.is_err());
    drop(login);
    import_native(LoginFamily::ChatgptCodex, &source, &path, true, LIMIT).unwrap();
    let login = open(&path).unwrap();
    assert_eq!(login.health().await.unwrap().0, LoginHealth::Expiring);
    assert!(login.request().await.is_ok());
    assert_eq!(
        fs::metadata(dir.path().join("store.renewal"))
            .unwrap()
            .len(),
        0
    );
}

#[test]
fn native_import_refuses_other_login_scopes_and_expired_tokens() {
    protect_process().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("auth.json");
    let path = dir.path().join("store");
    for value in [
        json!({"OPENAI_API_KEY":"synthetic-key"}),
        json!({"tokens":{"access_token":"s.eyJleHAiOjF9.s","refresh_token":"r","id_token":"i","account_id":"a"}}),
        json!({"auth_mode":"api_key","tokens":{"access_token":"s.eyJleHAiOjk5OTk5OTk5OTl9.s","refresh_token":"r","id_token":"i","account_id":"a"}}),
    ] {
        write(&source, &value);
        assert!(import_native(LoginFamily::ChatgptCodex, &source, &path, false, LIMIT).is_err());
    }
    let key = "https://auth.x.ai::b1a00492-073a-47ea-816f-4c329264a828";
    let entry = json!({"auth_mode":"oidc","oidc_issuer":"https://auth.x.ai","oidc_client_id":"b1a00492-073a-47ea-816f-4c329264a828","key":"synthetic-access","refresh_token":"synthetic-refresh","expires_at":"9999-12-31T00:00:00Z"});
    for field in ["auth_mode", "oidc_issuer", "oidc_client_id", "expires_at"] {
        let mut bad = entry.clone();
        bad[field] = json!("wrong");
        write(&source, &json!({key:bad}));
        assert!(
            import_native(LoginFamily::XaiGrok, &source, &path, false, LIMIT).is_err(),
            "accepted {field}"
        );
    }
    write(&source, &json!({"unscoped":entry}));
    assert!(import_native(LoginFamily::XaiGrok, &source, &path, false, LIMIT).is_err());
    write(&source, &json!({key:entry}));
    import_native(LoginFamily::XaiGrok, &source, &path, false, LIMIT).unwrap();
}

fn credential_binary() -> std::ffi::OsString {
    std::env::var_os("CARGO_BIN_EXE_symbiotic-credential-process").unwrap_or_else(|| {
        std::env::current_exe()
            .unwrap()
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("symbiotic-credential-process")
            .into_os_string()
    })
}

fn assert_secret_free_output(result: &std::process::Output, access: &str) {
    let output = format!(
        "{}{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    for secret in [
        access,
        "synthetic-refresh",
        "synthetic-id",
        "synthetic-account",
    ] {
        assert!(
            !output.contains(secret),
            "credential appeared in command output"
        );
    }
}

#[test]
fn import_command_never_prints_native_credentials() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("auth.json");
    let path = dir.path().join("store");
    let access = native(&source);
    let binary = credential_binary();
    let run = |extra: &[&str]| {
        Command::new(&binary)
            .args(["login", "import", "--family", "chatgpt_codex", "--native"])
            .arg(&source)
            .arg("--store")
            .arg(&path)
            .args(extra)
            .output()
            .unwrap()
    };
    let first = run(&[]);
    assert!(first.status.success());
    assert_secret_free_output(&first, &access);
    let refused = run(&[]);
    assert!(!refused.status.success());
    assert_secret_free_output(&refused, &access);
    let replacement = run(&["--replace"]);
    assert!(replacement.status.success());
    assert_secret_free_output(&replacement, &access);
}

#[test]
fn single_configuration_argument_reports_supported_secret_backends() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config");
    write(&config, &json!({"bad":"config"}));
    let result = Command::new(credential_binary())
        .arg(config)
        .output()
        .unwrap();
    assert!(!result.status.success());
    let message = String::from_utf8_lossy(&result.stderr);
    for backend in ["none", "owner_only_file", "login_file"] {
        assert!(
            message.contains(backend),
            "supported backend missing from configuration refusal"
        );
    }
}

#[tokio::test]
async fn failed_import_installation_fences_old_store_until_durable_replacement() {
    protect_process().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let store = dir.path().join("store");
    let native_path = dir.path().join("native");
    write(&store, &store_value());
    let original = fs::read(&store).unwrap();
    let access = format!(
        "synthetic.{}.signature",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(&json!({"exp":now()+3600,"padding":"x".repeat(512)})).unwrap()
        )
    );
    write(
        &native_path,
        &json!({"tokens":{
            "access_token":access,"refresh_token":"synthetic-new-refresh",
            "id_token":"synthetic-new-id","account_id":"synthetic-new-account"
        }}),
    );
    // The native input fits. The normalized store adds issuer/client/version
    // metadata and exceeds this same limit only during installation.
    let limit = fs::read(&native_path).unwrap().len();
    assert!(original.len() < limit);
    assert!(import_native(LoginFamily::ChatgptCodex, &native_path, &store, true, limit).is_err());
    assert_eq!(fs::read(&store).unwrap(), original);
    let login = open(&store).unwrap();
    assert_eq!(login.health().await.unwrap().0, LoginHealth::LoginRequired);
    assert!(login.request().await.is_err());
    drop(login);
    import_native(LoginFamily::ChatgptCodex, &native_path, &store, true, LIMIT).unwrap();
    let login = open(&store).unwrap();
    assert_eq!(login.health().await.unwrap().0, LoginHealth::Expiring);
    assert_eq!(
        login.request().await.unwrap().headers()["authorization"],
        format!("Bearer {access}")
    );
    assert!(
        fs::read(store.with_file_name("store.renewal"))
            .unwrap()
            .is_empty()
    );
}
