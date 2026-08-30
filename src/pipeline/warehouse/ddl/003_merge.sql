-- ---------------------------------------------------------------------------
-- The MERGE the loader runs, kept here so it can be read and tested on its own.
-- Placeholders in {braces} are filled by pipeline.warehouse.loader.
--
-- Why MERGE and not COPY straight into the target:
--   COPY INTO is append-only. Re-running a day would double the rows. The
--   primary key of the contract is (city_id, observed_at_utc), so an upsert on
--   that key makes a re-run a no-op when nothing changed and a correction when
--   the producer restated history.
-- ---------------------------------------------------------------------------

MERGE INTO {target} AS tgt
USING (
    SELECT
        $1:city_id::STRING                     AS city_id,
        $1:latitude::FLOAT                     AS latitude,
        $1:longitude::FLOAT                    AS longitude,
        $1:observed_at_utc::TIMESTAMP_NTZ      AS observed_at_utc,
        TO_DATE($1:observed_at_utc::TIMESTAMP_NTZ) AS observation_date,
        $1:temperature_2m_c::FLOAT             AS temperature_2m_c,
        $1:relative_humidity_2m_pct::FLOAT     AS relative_humidity_2m_pct,
        $1:precipitation_mm::FLOAT             AS precipitation_mm,
        $1:wind_speed_10m_kmh::FLOAT           AS wind_speed_10m_kmh,
        $1:weather_code::NUMBER                AS weather_code,
        $1:_ingested_at_utc::TIMESTAMP_NTZ     AS _ingested_at_utc,
        $1:_batch_id::STRING                   AS _batch_id,
        $1:_source_payload_hash::STRING        AS _source_payload_hash,
        $1:_contract_version::NUMBER           AS _contract_version
    FROM @{stage}/{stage_path}
    (FILE_FORMAT => RAW.FF_PARQUET)
) AS src
ON  tgt.CITY_ID = src.city_id
AND tgt.OBSERVED_AT_UTC = src.observed_at_utc

WHEN MATCHED AND tgt._SOURCE_PAYLOAD_HASH <> src._source_payload_hash THEN UPDATE SET
    tgt.LATITUDE                 = src.latitude,
    tgt.LONGITUDE                = src.longitude,
    tgt.OBSERVATION_DATE         = src.observation_date,
    tgt.TEMPERATURE_2M_C         = src.temperature_2m_c,
    tgt.RELATIVE_HUMIDITY_2M_PCT = src.relative_humidity_2m_pct,
    tgt.PRECIPITATION_MM         = src.precipitation_mm,
    tgt.WIND_SPEED_10M_KMH       = src.wind_speed_10m_kmh,
    tgt.WEATHER_CODE             = src.weather_code,
    tgt._INGESTED_AT_UTC         = src._ingested_at_utc,
    tgt._BATCH_ID                = src._batch_id,
    tgt._SOURCE_PAYLOAD_HASH     = src._source_payload_hash,
    tgt._CONTRACT_VERSION        = src._contract_version

WHEN NOT MATCHED THEN INSERT (
    CITY_ID, LATITUDE, LONGITUDE, OBSERVED_AT_UTC, OBSERVATION_DATE,
    TEMPERATURE_2M_C, RELATIVE_HUMIDITY_2M_PCT, PRECIPITATION_MM,
    WIND_SPEED_10M_KMH, WEATHER_CODE,
    _INGESTED_AT_UTC, _BATCH_ID, _SOURCE_PAYLOAD_HASH, _CONTRACT_VERSION
) VALUES (
    src.city_id, src.latitude, src.longitude, src.observed_at_utc, src.observation_date,
    src.temperature_2m_c, src.relative_humidity_2m_pct, src.precipitation_mm,
    src.wind_speed_10m_kmh, src.weather_code,
    src._ingested_at_utc, src._batch_id, src._source_payload_hash, src._contract_version
);
