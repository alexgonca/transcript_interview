CREATE EXTERNAL TABLE IF NOT EXISTS transcriptions.microsoft (
  timestamp_ms bigint,
  source string,
  timestamp string,
  durationInTicks bigint,
  duration string,
  combinedRecognizedPhrases array<
    struct<
      channel: int,
      lexical: string,
      itn: string,
      maskedITN: string,
      display: string
    >
  >,
  recognizedPhrases array<
    struct<
      recognitionStatus: string,
      channel: int,
      offset: string,
      duration: string,
      offsetInTicks: float,
      durationInTicks: float,
      nBest: array<
        struct<
          confidence: float,
          lexical: string,
          itn: string,
          maskedITN: string,
          display: string,
          words: array<
            struct<
              word: string,
              offset: string,
              duration: string,
              offsetInTicks: float,
              durationInTicks: float,
              confidence: float
            >
          >
        >
      >,
      speaker: int
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
) LOCATION 's3://transcriptions-agoncalves/transcript/service=microsoft/'
TBLPROPERTIES ('has_encrypted_data'='false');