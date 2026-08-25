# Contracts

One YAML file per data contract, versioned in the filename: `<name>.v<n>.yml`.

* A contract is never edited in place once it is `active` and has landed data in
  Snowflake. Breaking changes get a new file (`...v2.yml`) and a new RAW table
  suffix, so the two versions can coexist while staging is migrated.
* `pipeline.contracts.load_contract()` reads these files at runtime. Validation
  is driven by the YAML, so a contract change automatically changes what the
  pipeline accepts.
* The decision table for "is this change breaking?" lives in
  [docs/data_contract.md](../docs/data_contract.md).
