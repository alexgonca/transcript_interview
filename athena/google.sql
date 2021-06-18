CREATE EXTERNAL TABLE IF NOT EXISTS transcriptions.google (
    results array<
        struct<
            alternatives: array<
                struct<
                    transcript: string,
                    confidence: float,
                    words: array<
                        struct<
                            startTime: string,
                            endTime: string,
                            word: string,
                            speakerTag: int
                        >
                    >
                >
            >,
            languageCode: string
        >
    >,
    metadata_internet_scholar struct<
        started_at: timestamp,
        language: string,
        audio_storage: string,
        finished_at: timestamp
    >
)
PARTITIONED BY (project String, speaker String, performance_date String, part int, speaker_type String)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1',
  'ignore.malformed.json' = 'true'
) LOCATION 's3://transcriptions-agoncalves/transcript/service=google/'
TBLPROPERTIES ('has_encrypted_data'='false');