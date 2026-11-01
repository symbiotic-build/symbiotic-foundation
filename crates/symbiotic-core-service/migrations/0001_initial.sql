CREATE TABLE events (
 seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE, time TEXT NOT NULL,
 subject_kind TEXT NOT NULL, subject_id TEXT NOT NULL, kind TEXT NOT NULL, payload TEXT NOT NULL
);
CREATE TRIGGER events_no_update BEFORE UPDATE ON events BEGIN
 SELECT RAISE(ABORT, 'events are append-only'); END;
CREATE TRIGGER events_no_delete BEFORE DELETE ON events BEGIN
 SELECT RAISE(ABORT, 'events are append-only'); END;
CREATE TABLE jobs (
 id TEXT PRIMARY KEY, kind TEXT NOT NULL, product TEXT NOT NULL, inputs TEXT NOT NULL,
 rule_revision TEXT, model_route TEXT, budget_id TEXT, state TEXT NOT NULL,
 waiting_for TEXT NOT NULL, result_ref TEXT, created TEXT NOT NULL, updated TEXT NOT NULL
);
CREATE TABLE job_decisions (job_id TEXT NOT NULL, decision_id TEXT NOT NULL,
 PRIMARY KEY (job_id, decision_id));
CREATE TABLE decisions (
 id TEXT PRIMARY KEY, envelope TEXT NOT NULL, state TEXT NOT NULL, answer TEXT,
 answered_by TEXT, answered_at TEXT, budget_id TEXT, raised_by TEXT
);
CREATE TABLE budgets (
 id TEXT PRIMARY KEY, owner TEXT NOT NULL, account TEXT NOT NULL, name TEXT NOT NULL,
 unit TEXT NOT NULL, "limit" REAL NOT NULL, window TEXT NOT NULL, spent REAL NOT NULL,
 meter TEXT NOT NULL, state TEXT NOT NULL, warn_at REAL NOT NULL DEFAULT 0.9,
 over_limit_allowed INTEGER NOT NULL DEFAULT 0, read_at TEXT NOT NULL, updated TEXT NOT NULL
);
CREATE TABLE requests (
 request_id TEXT PRIMARY KEY, job_id TEXT NOT NULL, account TEXT NOT NULL, route TEXT NOT NULL,
 model TEXT NOT NULL, status INTEGER NOT NULL, refused TEXT, usage TEXT, meter TEXT NOT NULL, time TEXT NOT NULL
);
