CREATE EXTERNAL TABLE IF NOT EXISTS transcriptions.metadata (
    metadata_internet_scholar struct<
        started_at: timestamp,
        language: string,
        audio_storage: string,
        finished_at: timestamp
    >
)
PARTITIONED BY (service String, project String, speaker String, performance_date String, part int, speaker_type String)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1',
  'ignore.malformed.json' = 'true'
) LOCATION 's3://transcriptions-agoncalves/transcript/'
TBLPROPERTIES ('has_encrypted_data'='false');