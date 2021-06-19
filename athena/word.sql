CREATE EXTERNAL TABLE transcriptions.word (
  seq_num bigint,
  word string, 
  start_time bigint,
  end_time bigint)
PARTITIONED BY ( 
  project string, 
  speaker string, 
  performance_date string,
  part int,
  service string, 
  protagonist string,
  timeframe int,
  section int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION
  's3://transcriptions-agoncalves/word'