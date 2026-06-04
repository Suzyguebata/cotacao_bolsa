from datetime import datetime, timezone

from scheduler import create_scheduler


def test_scheduler_runs_collection_immediately_on_start():
    scheduler = create_scheduler()
    jobs = scheduler.get_jobs()

    assert len(jobs) == 1
    assert jobs[0].trigger.interval.total_seconds() == 300

    now = datetime.now(timezone.utc)
    delay_seconds = abs((jobs[0].next_run_time - now).total_seconds())
    assert delay_seconds < 5
