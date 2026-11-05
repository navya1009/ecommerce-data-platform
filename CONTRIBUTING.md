# Contributing

Thanks for working on the e-commerce data platform. This guide covers how changes move from a branch to `main`, how to add a data source or a Spark job, and what a reviewer checks.

## 1. Branching

We use GitFlow.

| Branch | Purpose | Merges into |
|---|---|---|
| `main` | Production. Every commit here is deployable. | — |
| `develop` | Integration branch. Feature work lands here first. | `main` (release) |
| `feature/*` | One piece of work, branched from `develop`. | `develop` |
| `hotfix/*` | Urgent fix to production, branched from `main`. | `main` and `develop` |

**Current state:** the repository has only `feature/setup-project-structure`. Create `develop` from it, and `main` from `develop` once it is stable, before using this flow.

```bash
git switch -c develop
git push -u origin develop
git switch -c feature/add-inventory-source develop
```

Keep branches short-lived. Rebase onto `develop` before opening a pull request if `develop` has moved.

## 2. Commit messages

Use [Conventional Commits](https://www.conventionalcommits.org/): `type(scope): summary`.

| Type | Use for |
|---|---|
| `feat` | New behaviour or a new capability |
| `fix` | A bug fix |
| `chore` | Build, tooling, dependency bumps, housekeeping |
| `docs` | Documentation only |
| `test` | Adding or fixing tests, no production change |
| `refactor` | Restructuring with no behaviour change |

Examples:

- `feat(analytics): add cohort retention query`
- `fix(spark): use psycopg3 for fact_orders upsert`
- `fix(clickstream): align fact_clickstream columns with migration 007`
- `docs(deployment): add rollback procedure`
- `test(integration): cover lineage writes from the orders transform`

Keep the summary under about 72 characters, in the imperative mood. Put the reason for the change in the body.

## 3. Adding a new data source

Work through these steps in order. Each one has a test or a check you can run.

1. **Write the extractor.** Add a file under `src/extract/`, following `ingest_csv.py` or `ingest_api.py`. Use `get_logger(__name__)` from `src/utils/logger.py` for new code.
2. **Reuse shared helpers.** If the source needs watermarks, batch IDs or pipeline-run tracking, use `src/utils/ingestion_helpers.py`. Do not change the existing `setup_logging` function. Add new helpers beside it.
3. **Add a staging table.** Create a new migration under `migrations/mysql/` with the next free number. Never edit an existing migration. The staging table needs `batch_id`, `is_processed`, `ingested_at`, and a watermark column if the source is incremental.
4. **Record lineage.** After a successful load, call `record_lineage(...)` from `src/utils/lineage.py`. Wrap the call in `try/except` and log a warning on failure, so a lineage outage cannot fail ingestion. Record only batches that wrote rows.
5. **Add unit tests** in `tests/unit/test_ingestion.py`. Mock the database and the source. Cover the parsing, the watermark logic and the lineage call.
6. **Add an integration test** in `tests/integration/test_full_pipeline.py`. Seed the source table in the test Postgres or MySQL container, run the extractor, and assert on the staging rows.
7. **Update the DAG** in `dags/ecommerce_pipeline_dag.py` so the new extractor runs in order, with the existing retry and alert callbacks.

Before you open the pull request, run `make test-unit` and `make test-integration`.

## 4. Adding a new Spark job

1. **Create the job** as `src/spark_jobs/transform_<entity>.py`. Follow `transform_orders.py`: read connection settings from environment variables with the Docker defaults, and keep credentials in one place.
2. **Make the module importable under spark-submit.** Add the same `sys.path` line the existing jobs use, so `utils/` can be imported.
3. **Use the logging and metrics wrappers.** Set `logger = get_logger(__name__)`. Wrap the job's entry point in `track_metrics(pipeline_name=..., run_id=...)`, and set the row counts (`rows_loaded`, `rows_quarantined`, and so on) on the metrics object.
4. **Record lineage** after the successful write, in its own `try/except`.
5. **Add unit tests** in `tests/unit/test_spark_transforms.py`. Test the pure transformation functions on small DataFrames where possible, and mock the JDBC reads and writes.
6. **Add an integration test** that runs the job end to end against the test containers, as `test_clickstream_transform_loads_fact_clickstream` does. Let a job failure fail the test. Only a `SyntaxError` in the job (the known encoding problem) may skip it.
7. **Register the job in the DAG** as a new task, after the upstream task it depends on, with the same `spark-submit` arguments the other tasks use.

## 5. Pull request checklist

Reviewers check these before approving. Tick them in the pull request description.

- [ ] Unit tests pass locally (`make test-unit`)
- [ ] Integration tests pass locally (`make test-integration`)
- [ ] Pre-commit hooks pass (`pre-commit run --all-files`)
- [ ] No secrets committed. Search the diff for passwords, keys and webhook URLs.
- [ ] Migration files are numbered and ordered, and no existing migration was edited
- [ ] Lineage is recorded for new data flows
- [ ] Metrics are tracked in new transform jobs
- [ ] `README.md` or `deployment/RUNBOOK.md` is updated if behaviour changed
- [ ] Coverage has not dropped below 80%

**Known gap:** coverage is currently about 19% for `src/` (unit tests only, `make coverage`). The 80% item cannot pass as things stand. Either raise coverage in the same change, or note the gap in the pull request and agree a target with the reviewer.

## 6. Working on the codebase

- Use `psycopg` (psycopg3) for new database code. Do not add `psycopg2`.
- Do not re-encode files that were corrupted by a UTF-16 byte-order mark, such as `src/spark_jobs/__init__.py`.
- Keep the Spark jobs and the DAG working with their Docker defaults. Environment variables may override the defaults, but the defaults must not change.
- Run `pre-commit run --all-files` before you push. It runs black, flake8 and the other hooks.
