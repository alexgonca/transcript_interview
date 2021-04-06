from pydub import AudioSegment
from pathlib import Path
import shutil
import uuid
from internet_scholar import read_dict_from_s3, s3_key_exists, save_data_in_s3, instantiate_ec2, AthenaDatabase
from collections import OrderedDict
from transcriber_parser import parse_words
import csv
import os

SELECT_TRANSCRIPT = """with updated_word as
(select
       start_time / ({interval_in_seconds} * 1000) as time_frame,
       if(protagonist='1', word, upper(word)) as word,
       start_time,
       end_time,
       service
from transcriptions.word
where
      project = '{project}' and
      speaker = '{speaker}' and
      performance_date = '{performance_date}'
order by start_time, seq_num)
select
    time '00:00:00' + time_frame * {interval_in_seconds} * interval '1' second as time_frame,
    array_join(array_remove(array_agg(if(service='microsoft', word, '')), ''), ' ') as microsoft,
    array_join(array_remove(array_agg(if(service='google', word, '')), ''), ' ') as google,
    array_join(array_remove(array_agg(if(service='aws', word, '')), ''), ' ') as aws,
    array_join(array_remove(array_agg(if(service='ibm', word, '')), ''), ' ') as ibm,
    '' as comments
from updated_word
group by time_frame
order by time_frame"""

SELECT_ALL_TRANSCRIPTS = """select distinct project, speaker, performance_date
from word {where_clause} order by project, speaker, performance_date"""

SELECT_NON_PARSED_TRANSCRIPTS = """select distinct project, speaker, performance_date, speaker_type, service
from metadata
{where_clause}
    and not exists(
          select *
          from word
          where word.project = metadata.project and
                word.speaker = metadata.speaker and
                word.performance_date = metadata.performance_date and
                word.protagonist = if(metadata.speaker_type='interviewer', '0', '1') and 
                word.service = metadata.service)
order by project, speaker, service, speaker_type"""


class Transcript:
    def __init__(self, bucket):
        self.bucket = bucket
        self.config = read_dict_from_s3(bucket=self.bucket, key='config/config.json')

    def instantiate_cloud_transcriber(self, service, project, performance_date,
                                      language, speaker, speaker_type, filepath, original_file=None):
        print(f"{speaker}_{service}_{speaker_type}")
        size = 8
        if service == "microsoft":
            from transcribe_microsoft import upload_audio_file, delete_uploaded_file
        elif service == "google":
            from transcribe_google import upload_audio_file, delete_uploaded_file
        elif service == "aws":
            from transcribe_aws import upload_audio_file, delete_uploaded_file
        elif service == "ibm":
            from transcribe_ibm import upload_audio_file, delete_uploaded_file
            size = 10
            if Path(filepath).stat().st_size >= 1073741824:
                extension = Path(original_file).suffix[1:]
                sound = AudioSegment.from_file(original_file, extension)
                sound = sound.set_channels(1)
                Path('./audio/').mkdir(parents=True, exist_ok=True)
                filepath = f"./audio/{uuid.uuid4()}.mp3"
                sound.export(filepath, format="mp3")
        else:
            raise Exception(f"Invalid service: {service}")

        identifier = upload_audio_file(filepath=filepath, service_config=self.config[service])
        try:
            parameters = f"{self.bucket} {identifier} {language} {speaker} {speaker_type} " \
                         f"{performance_date} {project} {service}"
            instantiate_ec2(ami=self.config['aws']['ami'],
                            key_name=self.config['aws']['key_name'],
                            security_group=self.config['aws']['security_group'],
                            iam=self.config['aws']['iam'],
                            parameters=parameters,
                            instance_type='t3a.nano',
                            size=size,
                            init_script="https://raw.githubusercontent.com/alexgonca/transcript_interview/main/init_server.sh",
                            name=f"{speaker}_{service}_{speaker_type}")
        except:
            delete_uploaded_file(identifier=identifier, service_config=self.config[service])
            raise

    def retrieve_transcript(self, project, speaker, performance_date, language=None,
                            both=None, single=None, interviewee=None, interviewer=None,
                            microsoft=False, ibm=False, aws=False, google=False):
        if single is not None:
            self.inner_retrieve_transcript(project=project, speaker=speaker, performance_date=performance_date,
                                           speaker_type='single', language=language, filepath=single,
                                           microsoft=microsoft, ibm=ibm, aws=aws, google=google)
        if both is not None:
            self.inner_retrieve_transcript(project=project, speaker=speaker, performance_date=performance_date,
                                           speaker_type='both', language=language, filepath=both,
                                           microsoft=microsoft, ibm=ibm, aws=aws, google=google)
        if interviewee is not None:
            self.inner_retrieve_transcript(project=project, speaker=speaker, performance_date=performance_date,
                                           speaker_type='interviewee', language=language, filepath=interviewee,
                                           microsoft=microsoft, ibm=ibm, aws=aws, google=google)
        if interviewer is not None:
            self.inner_retrieve_transcript(project=project, speaker=speaker, performance_date=performance_date,
                                           speaker_type='interviewer', language=language, filepath=interviewer,
                                           microsoft=microsoft, ibm=ibm, aws=aws, google=google)

    def inner_retrieve_transcript(self, project, speaker, performance_date,
                                  speaker_type, language=None, filepath=None,
                                  microsoft=False, ibm=False, aws=False, google=False):
        retrieved = {
            'microsoft': False,
            'google': False,
            'aws': False,
            'ibm': False
        }
        if microsoft:
            retrieved['microsoft'] = s3_key_exists(self.bucket,
                                                   f'transcript/service=microsoft/project={project}/speaker={speaker}/'
                                                   f'performance_date={performance_date}/speaker_type={speaker_type}/transcript.json.bz2')
        if google:
            retrieved['google'] = s3_key_exists(self.bucket,
                                                f'transcript/service=google/project={project}/speaker={speaker}/'
                                                f'performance_date={performance_date}/speaker_type={speaker_type}/transcript.json.bz2')
        if aws:
            retrieved['aws'] = s3_key_exists(self.bucket,
                                             f'transcript/service=aws/project={project}/speaker={speaker}/'
                                             f'performance_date={performance_date}/speaker_type={speaker_type}/transcript.json.bz2')
        if ibm:
            retrieved['ibm'] = s3_key_exists(self.bucket,
                                             f'transcript/service=ibm/project={project}/speaker={speaker}/'
                                             f'performance_date={performance_date}/speaker_type={speaker_type}/transcript.json.bz2')

        destination = ""
        created_audio = False
        if (microsoft and not retrieved["microsoft"]) or (google and not retrieved["google"]) or \
                (aws and not retrieved["aws"]) or (ibm and not retrieved["ibm"]):
            extension = Path(filepath).suffix[1:]
            sound = AudioSegment.from_file(filepath, extension)
            sound = sound.set_channels(1)
            Path('./audio/').mkdir(parents=True, exist_ok=True)
            destination = f"./audio/{uuid.uuid4()}.wav"
            sound.export(destination, format="wav")
            created_audio = True
        try:
            if microsoft and not retrieved['microsoft']:
                self.instantiate_cloud_transcriber(service="microsoft",
                                                   project=project,
                                                   performance_date=performance_date,
                                                   language=language,
                                                   speaker=speaker,
                                                   speaker_type=speaker_type,
                                                   filepath=destination)
            if google and not retrieved['google']:
                self.instantiate_cloud_transcriber(service="google",
                                                   project=project,
                                                   performance_date=performance_date,
                                                   language=language,
                                                   speaker=speaker,
                                                   speaker_type=speaker_type,
                                                   filepath=destination)
            if ibm and not retrieved['ibm']:
                self.instantiate_cloud_transcriber(service="ibm",
                                                   project=project,
                                                   performance_date=performance_date,
                                                   language=language,
                                                   speaker=speaker,
                                                   speaker_type=speaker_type,
                                                   filepath=destination,
                                                   original_file=filepath)
            if aws and not retrieved['aws']:
                self.instantiate_cloud_transcriber(service="aws",
                                                   project=project,
                                                   performance_date=performance_date,
                                                   language=language,
                                                   speaker=speaker,
                                                   speaker_type=speaker_type,
                                                   filepath=destination)
        finally:
            if created_audio:
                shutil.rmtree("./audio")

    def get_where_clause(self, project=None, speaker=None, performance_date=None):
        where_clause = ""
        if project is not None:
            where_clause = f"AND project = '{project}' "
        if speaker is not None:
            where_clause = f"{where_clause}AND speaker = '{speaker}' "
        if performance_date is not None:
            where_clause = f"{where_clause}AND performance_date = '{performance_date}' "
        if where_clause != '':
            where_clause = f"where {where_clause[4:]}"
        return where_clause

    def parse_words(self, project=None, speaker=None, performance_date=None):
        athena_db = AthenaDatabase(database=self.config['aws']['athena'], s3_output=self.bucket)
        athena_db.query_athena_and_wait(query_string="MSCK REPAIR TABLE metadata")

        where_clause = self.get_where_clause(project=project, speaker=speaker, performance_date=performance_date)
        unparsed_records = athena_db.query_athena_and_download(
            query_string=SELECT_NON_PARSED_TRANSCRIPTS.format(where_clause=where_clause),
            filename='unparsed_records.csv')
        with open(unparsed_records) as unparsed_file:
            reader = csv.DictReader(unparsed_file)
            database_has_changed = False
            try:
                for row in reader:
                    transcript = read_dict_from_s3(self.bucket,
                                                   f"transcript/service={row['service']}/project={project}/speaker={speaker}/"
                                                   f"performance_date={performance_date}/speaker_type={row['speaker_type']}/transcript.json.bz2",
                                                   compressed=True)
                    protagonist_words, non_protagonist_words = parse_words(transcript=transcript,
                                                                           speaker_type=row['speaker_type'],
                                                                           service=row['service'])
                    partitions = OrderedDict()
                    partitions['project'] = project
                    partitions['speaker'] = speaker
                    partitions['performance_date'] = performance_date
                    partitions['service'] = row['service']
                    if len(protagonist_words) > 0:
                        partitions['protagonist'] = 1
                        save_data_in_s3(content=protagonist_words,
                                        s3_bucket=self.bucket,
                                        s3_key='word.json',
                                        prefix='word',
                                        partitions=partitions)
                    if len(non_protagonist_words) > 0:
                        partitions['protagonist'] = 0
                        save_data_in_s3(content=non_protagonist_words,
                                        s3_bucket=self.bucket,
                                        s3_key='word.json',
                                        prefix='word',
                                        partitions=partitions)
                        database_has_changed = True
            finally:
                if database_has_changed:
                    athena_db.query_athena_and_wait(query_string="MSCK REPAIR TABLE word")

    def export_csv(self, project=None, speaker=None, performance_date=None, interval_in_seconds=10):
        self.parse_words(project=project, speaker=speaker, performance_date=performance_date)

        athena_db = AthenaDatabase(database=self.config['aws']['athena'], s3_output=self.bucket)
        where_clause = self.get_where_clause(project=project, speaker=speaker, performance_date=performance_date)
        tmp_file = athena_db.query_athena_and_download(
            query_string=SELECT_ALL_TRANSCRIPTS.format(where_clause=where_clause),
            filename='selected_transcripts.csv')
        with open(tmp_file) as csvfile:
            reader = csv.DictReader(csvfile)
            Path("./csv/").mkdir(parents=True, exist_ok=True)
            for row in reader:
                filename = f"{row['project']}_{row['speaker']}_{row['performance_date']}_{interval_in_seconds}.csv"
                new_file = athena_db.query_athena_and_download(SELECT_TRANSCRIPT.format(project=row['project'],
                                                                                        speaker=row['speaker'],
                                                                                        performance_date=row[
                                                                                            'performance_date'],
                                                                                        interval_in_seconds=interval_in_seconds),
                                                               filename)
                os.replace(new_file, f'./csv/{filename}')
