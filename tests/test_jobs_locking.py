from datetime import datetime, timedelta, timezone

from src import jobs, utils


def test_next_due_jobs_skips_running_jobs(temp_certiva_env):
    job_id = utils.create_job("demo", "scan_folder", tenant="demo", config={"path": "IN/demo"}, schedule="every_5m", enabled=True)
    jobs = utils.next_due_jobs()
    assert any(job["id"] == job_id for job in jobs)
    utils.mark_job_started(job_id, "test-host")
    # Should skip while run_started_at is recent
    jobs = utils.next_due_jobs()
    assert all(job["id"] != job_id for job in jobs)
    # Simulate 20 minutes later by manually updating
    past = (datetime.now(timezone.utc) - timedelta(minutes=20)).isoformat()
    with utils.get_connection() as conn:
        conn.execute("UPDATE jobs SET run_started_at = ? WHERE id = ?", (past, job_id))
    jobs = utils.next_due_jobs()
    assert any(job["id"] == job_id for job in jobs)


def test_run_job_updates_status(temp_certiva_env, monkeypatch):
    job_id = utils.create_job("preflight", "run_preflight", tenant="demo", config={}, schedule="every_5m", enabled=True)
    monkeypatch.setattr(jobs.metrics, "print_preflight", lambda tenant=None: None)
    job = utils.get_job(job_id)
    jobs.run_job(job)
    updated = utils.get_job(job_id)
    assert updated["last_status"] == "success"


def test_run_job_marks_skipped_on_jobskipped(temp_certiva_env):
    cfg = {"tenant": "demo", "host": "imap.example.com", "username": "user", "password": "secret"}
    job_id = utils.create_job("imap", "scan_imap", tenant="demo", config=cfg, schedule="every_5m", enabled=True)
    job = utils.get_job(job_id)
    jobs.run_job(job)
    updated = utils.get_job(job_id)
    assert updated["last_status"] == "skipped"
