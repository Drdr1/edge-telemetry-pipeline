from pipelines import defs


def test_definitions_load_and_jobs_resolve():
    for name in ("hourly_rollup", "daily_open_data", "cost_model"):
        assert defs.resolve_job_def(name) is not None


def test_schedules_registered():
    names = {s.name for s in defs.schedules}
    assert len(names) == 3
