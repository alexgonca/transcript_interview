CREATE EXTERNAL TABLE IF NOT EXISTS transcriptions.ibm (
    created string,
    id string,
    updated string,
    results array<
        struct<
            result_index: int,
            results: array<
                struct<
                    final: boolean,
                    alternatives: array<
                        struct<
                            transcript: string,
                            timestamps: array<
                                array<string>
                            >,
                            confidence: float,
                            word_confidence: array<
                                array<string>
                            >
                       >
                    >
                >
            >
        >
    >,
    status string,
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
) LOCATION 's3://transcriptions-agoncalves/transcript/service=ibm/'
TBLPROPERTIES ('has_encrypted_data'='false');