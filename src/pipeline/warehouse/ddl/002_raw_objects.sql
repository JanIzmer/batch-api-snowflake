-- ---------------------------------------------------------------------------
-- RAW + OPS objects. Idempotent: safe to re-run on every deploy.
-- ---------------------------------------------------------------------------

USE DATABASE WEATHER;

CREATE FILE FORMAT IF NOT EXISTS RAW.FF_PARQUET
  TYPE = PARQUET
  COMPRESSION = SNAPPY;

-- Internal stage. The pipeline PUTs local parquet here; in a cloud deployment
-- this becomes an external stage over the landing bucket and the PUT goes away.
CREATE STAGE IF NOT EXISTS RAW.STG_WEATHER_OBSERVATION
  FILE_FORMAT = RAW.FF_PARQUET
  COMMENT = 'Landing zone for weather_observation v1';

CREATE TABLE IF NOT EXISTS RAW.WEATHER_OBSERVATION (
    CITY_ID                   STRING        NOT NULL,
    LATITUDE                  FLOAT         NOT NULL,
    LONGITUDE                 FLOAT         NOT NULL,
    OBSERVED_AT_UTC           TIMESTAMP_NTZ NOT NULL,
    OBSERVATION_DATE          DATE          NOT NULL,
    TEMPERATURE_2M_C          FLOAT,
    RELATIVE_HUMIDITY_2M_PCT  FLOAT,
    PRECIPITATION_MM          FLOAT,
    WIND_SPEED_10M_KMH        FLOAT,
    WEATHER_CODE              NUMBER(38,0),
    _INGESTED_AT_UTC          TIMESTAMP_NTZ NOT NULL,
    _BATCH_ID                 STRING        NOT NULL,
    _SOURCE_PAYLOAD_HASH      STRING        NOT NULL,
    _CONTRACT_VERSION         NUMBER(38,0)  NOT NULL,
    CONSTRAINT PK_WEATHER_OBSERVATION PRIMARY KEY (CITY_ID, OBSERVED_AT_UTC) RELY
)
-- Snowflake micro-partitions are automatic; clustering only pays off past a few
-- hundred GB. At this volume OBSERVATION_DATE is already well correlated with
-- load order, so no CLUSTER BY here - see docs/cost_model.md.
COMMENT = 'weather_observation contract v1, grain = (city_id, observed_at_utc)';

-- Every load leaves a row here. This is what the incident runbook reads first.
CREATE TABLE IF NOT EXISTS OPS.LOAD_AUDIT (
    RUN_ID              STRING        NOT NULL,
    DATASET             STRING        NOT NULL,
    CONTRACT_VERSION    NUMBER(38,0)  NOT NULL,
    CITY_ID             STRING        NOT NULL,
    OBSERVATION_DATE    DATE          NOT NULL,
    BATCH_ID            STRING        NOT NULL,
    SOURCE_PAYLOAD_HASH STRING,
    ROWS_FETCHED        NUMBER(38,0),
    ROWS_ACCEPTED       NUMBER(38,0),
    ROWS_REJECTED       NUMBER(38,0),
    ROWS_INSERTED       NUMBER(38,0),
    ROWS_UPDATED        NUMBER(38,0),
    STATUS              STRING        NOT NULL,   -- loaded | skipped_unchanged | failed
    ERROR_MESSAGE       STRING,
    STARTED_AT_UTC      TIMESTAMP_NTZ NOT NULL,
    FINISHED_AT_UTC     TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS OPS.QUARANTINE (
    QUARANTINED_AT_UTC TIMESTAMP_NTZ NOT NULL,
    DATASET            STRING        NOT NULL,
    CONTRACT_VERSION   NUMBER(38,0)  NOT NULL,
    CITY_ID            STRING,
    OBSERVATION_DATE   DATE,
    REASONS            ARRAY,
    PAYLOAD            VARIANT
);
