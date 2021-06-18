CREATE EXTERNAL TABLE transcriptions.word (
  seq_num string, 
  word string, 
  start_time string, 
  end_time string)
PARTITIONED BY ( 
  project string, 
  speaker string, 
  performance_date string,
  part int,
  service string, 
  protagonist string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION
  's3://transcriptions-agoncalves/word'