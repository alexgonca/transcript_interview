select project, speaker, performance_date, speaker_type,
       sum(if(service='microsoft', 1, 0)) as microsoft,
       sum(if(service='google', 1, 0)) as google,
       sum(if(service='aws', 1, 0)) as aws,
       sum(if(service='ibm', 1, 0)) as ibm
from transcriptions.metadata
group by project, speaker, performance_date, speaker_type
order by project, speaker, performance_date, speaker_type;