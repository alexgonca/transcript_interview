CREATE EXTERNAL TABLE IF NOT EXISTS transcriptions.aws (
    jobName string,
    accountId string,
    results struct<
        transcripts: array<
            struct<
                transcript: string
            >
        >,
        items: array<
            struct<
                start_time: string,
                end_time: string,
                alternatives: array<
                    struct<
                        confidence: string,
                        content: string
                    >
                >,
                type: string
            >
        >,
        speaker_labels: struct<
            speakers: int,
            segments: array<
                struct<
                    start_time: string,
                    speaker_label: string,
                    end_time: string,
                    items: array<
                        struct<
                            start_time: string,
                            speaker_label: string,
                            end_time: string
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
) LOCATION 's3://transcriptions-agoncalves/transcript/service=aws/'
TBLPROPERTIES ('has_encrypted_data'='false');