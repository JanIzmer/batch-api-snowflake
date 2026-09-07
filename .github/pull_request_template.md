## What changed

## Data impact

- [ ] Does this change a data contract? If yes, link the new contract version.
- [ ] Does this change the grain or the primary key of a model?
- [ ] Does it need a backfill? If yes, which range, and who runs it?
- [ ] Are new columns covered by tests (`not_null`, `unique`, ranges)?

## Cost impact

- [ ] Rough change in warehouse seconds per run:
- [ ] Any new full table scan or full refresh introduced?

## Verification

- [ ] `make test`
- [ ] `dbt build` against a dev target
