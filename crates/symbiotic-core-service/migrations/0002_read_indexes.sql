CREATE INDEX events_subject ON events(subject_kind, subject_id);
CREATE INDEX requests_job ON requests(job_id);
CREATE INDEX requests_account_time ON requests(account, time);
CREATE INDEX jobs_created ON jobs(created);
CREATE INDEX budgets_account ON budgets(account);
CREATE INDEX decisions_budget_state ON decisions(budget_id, state);
CREATE INDEX requests_time ON requests(time);
