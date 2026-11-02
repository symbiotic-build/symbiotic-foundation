mod common;
use rusqlite::{Connection, params};
use serde_json::json;
use symbiotic_core_service::Store;
#[test]
fn version_one_job_survives_migration_and_events_remain_append_only() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("core.sqlite");
    drop(Store::open_version(&path, 1).unwrap());
    let conn = Connection::open(&path).unwrap();
    conn.execute("INSERT INTO jobs VALUES ('job','review','test','[]',NULL,NULL,NULL,'ready','[]',NULL,?1,?1)",[common::time()]).unwrap();
    conn.execute("INSERT INTO events(id,time,subject_kind,subject_id,kind,payload) VALUES ('event',?1,'job','job','job.created','{}')",[common::time()]).unwrap();
    let mut store = Store::open(dir.path()).unwrap();
    assert_eq!(
        conn.pragma_query_value::<u32, _>(None, "user_version", |r| r.get(0))
            .unwrap(),
        2
    );
    let result = common::ok(
        &mut store,
        "job.transition",
        json!({"id":"job","to":"running"}),
    );
    assert_eq!(result["job"]["product"], "test");
    for sql in [
        "UPDATE events SET kind='changed' WHERE id=?1",
        "DELETE FROM events WHERE id=?1",
    ] {
        assert!(
            conn.execute(sql, params!["event"])
                .unwrap_err()
                .to_string()
                .contains("events are append-only")
        );
    }
    assert_eq!(common::events(&mut store).len(), 2);
}
#[test]
fn future_store_version_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("core.sqlite");
    let conn = Connection::open(&path).unwrap();
    conn.pragma_update(None, "user_version", 99).unwrap();
    let error = Store::open(dir.path()).err().unwrap();
    assert_eq!(error.reason, "store_version_newer");
}
