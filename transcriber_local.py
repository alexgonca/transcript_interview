from pydub import AudioSegment
from pydub.utils import make_chunks
from pathlib import Path
import shutil
import uuid
from internet_scholar import read_dict_from_s3, s3_prefix_exists, delete_s3_objects_by_prefix, save_data_in_s3, instantiate_ec2, AthenaDatabase, move_data_in_s3
from collections import OrderedDict
from transcriber_parser import parse_words
import csv
import os

SELECT_TRANSCRIPT = """with updated_word as
(select
       ( start_time + ( (section-1)*timeframe*60*60*1000 ) ) / ({interval_in_seconds} * 1000) as time_slot,
       if(protagonist='1', word, upper(word)) as word,
       start_time,
       end_time,
       service
from transcriptions.word
where
      project = '{project}' and
      speaker = '{speaker}' and
      performance_date = '{performance_date}' and 
      part = {part}
order by section, start_time, seq_num)
select
    time '00:00:00' + time_slot * {interval_in_seconds} * interval '1' second as time_slot,
    array_join(array_remove(array_agg(if(service='microsoft', word, '')), ''), ' ') as microsoft,
    array_join(array_remove(array_agg(if(service='google', word, '')), ''), ' ') as google,
    array_join(array_remove(array_agg(if(service='aws', word, '')), ''), ' ') as aws,
    array_join(array_remove(array_agg(if(service='ibm', word, '')), ''), ' ') as ibm,
    '' as comments
from updated_word
group by time_slot
order by time_slot"""

SELECT_ALL_TRANSCRIPTS = """select distinct project, speaker, performance_date, part
from word {where_clause} order by project, speaker, performance_date, part"""

SELECT_NON_PARSED_TRANSCRIPTS = """select distinct project, speaker, performance_date, part, speaker_type, timeframe, section, service
from metadata
{where_clause}
    and not exists(
          select *
          from word
          where word.project = metadata.project and
                word.speaker = metadata.speaker and
                word.performance_date = metadata.performance_date and
                word.part = metadata.part and
                word.protagonist = if(metadata.speaker_type='interviewer', '0', '1') and 
                word.service = metadata.service and 
                word.timeframe = metadata.timeframe and
                word.section = metadata.section)
order by project, speaker, service, speaker_type"""

SELECT_JOBS = """with job as (
    select *
    from (values {jobs_values}) 
    AS t(service, project, speaker, performance_date, part, speaker_type, timeframe, section)
)
select service, project, speaker, performance_date, part, speaker_type, timeframe, section
from job
where not exists (
    select *
    from metadata
    where
          metadata.service = job.service and
          metadata.project = job.project and
          metadata.speaker = job.speaker and
          metadata.performance_date = job.performance_date and
          metadata.speaker_type = job.speaker_type and
          metadata.part = job.part and
          metadata.speaker_type = job.speaker_type and
          metadata.timeframe = job.timeframe and
          metadata.section = job.section
    )"""


class Transcript:
    def __init__(self, bucket):
        self.instance_type = 't3a.nano'
        self.bucket = bucket
        self.config = read_dict_from_s3(bucket=self.bucket, key='config/config.json')
        self.repair_metadata = True

    def instantiate_cloud_transcriber(self, service, project, performance_date, part, timeframe, section,
                                      language, speaker, speaker_type, filepath):
        print(f"{speaker}_{service}_{speaker_type}_{part}_{section}")
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
                extension = Path(filepath).suffix[1:]
                sound = AudioSegment.from_file(filepath, extension)
                sound = sound.set_channels(1)
                Path('./audio/').mkdir(parents=True, exist_ok=True)
                filepath = f"./audio/{uuid.uuid4()}.mp3"
                sound.export(filepath, format="mp3")
        else:
            raise Exception(f"Invalid service: {service}")

        identifier = upload_audio_file(filepath=filepath, service_config=self.config[service])
        try:
            parameters = f"{self.bucket} {identifier} {language} {speaker} {speaker_type} " \
                         f"{performance_date} {part} {timeframe} {section} {project} {service}"
            instantiate_ec2(ami=self.config['aws']['ami'],
                            key_name=self.config['aws']['key_name'],
                            security_group=self.config['aws']['security_group'],
                            iam=self.config['aws']['iam'],
                            parameters=parameters,
                            instance_type=self.instance_type,
                            size=size,
                            init_script="https://raw.githubusercontent.com/alexgonca/transcript_interview/main/init_server.sh",
                            name=f"{speaker}_{part}_{service}_{speaker_type}_{section}")
        except:
            delete_uploaded_file(identifier=identifier, service_config=self.config[service])
            raise

    def retrieve_transcript(self, project, speaker, performance_date, part=1, timeframe=3, language=None,
                            both=None, single=None, interviewee=None, interviewer=None,
                            microsoft=False, ibm=False, aws=False, google=False):
        if single is not None:
            self.inner_retrieve_transcript(project=project, speaker=speaker, performance_date=performance_date,
                                           speaker_type='single', part=part, timeframe=timeframe,
                                           language=language, filepath=single,
                                           microsoft=microsoft, ibm=ibm, aws=aws, google=google)
        if both is not None:
            self.inner_retrieve_transcript(project=project, speaker=speaker, performance_date=performance_date,
                                           speaker_type='both', part=part, timeframe=timeframe,
                                           language=language, filepath=both,
                                           microsoft=microsoft, ibm=ibm, aws=aws, google=google)
        if interviewee is not None:
            self.inner_retrieve_transcript(project=project, speaker=speaker, performance_date=performance_date,
                                           speaker_type='interviewee', part=part, timeframe=timeframe,
                                           language=language, filepath=interviewee,
                                           microsoft=microsoft, ibm=ibm, aws=aws, google=google)
        if interviewer is not None:
            self.inner_retrieve_transcript(project=project, speaker=speaker, performance_date=performance_date,
                                           speaker_type='interviewer', part=part, timeframe=timeframe,
                                           language=language, filepath=interviewer,
                                           microsoft=microsoft, ibm=ibm, aws=aws, google=google)

    def delete_different_timeframe(self, service, project, speaker, performance_date, speaker_type, part, timeframe):
        prefix = f"transcript/service={service}/project={project}/speaker={speaker}/" \
                 f"performance_date={performance_date}/part={part}/speaker_type={speaker_type}/"
        if s3_prefix_exists(bucket=self.bucket, prefix=prefix):
            if not s3_prefix_exists(bucket=self.bucket, prefix=f"{prefix}timeframe={timeframe}/"):
                delete_s3_objects_by_prefix(bucket=self.bucket, prefix=prefix)
                prefix_word = f"word/project={project}/speaker={speaker}/performance_date={performance_date}/" \
                              f"part={part}/service={service}/"
                if speaker_type in ('interviewee', 'single'):
                    prefix_word = f"{prefix_word}protagonist=1/"
                elif speaker_type == 'interviewer':
                    prefix_word = f"{prefix_word}protagonist=0/"
                delete_s3_objects_by_prefix(bucket=self.bucket, prefix=prefix_word)
                self.repair_metadata = True

    def inner_retrieve_transcript(self, project, speaker, performance_date,
                                  speaker_type, part, timeframe, language, filepath,
                                  microsoft, ibm, aws, google):
        athena_db = AthenaDatabase(database=self.config['aws']['athena'], s3_output=self.bucket)

        # delete existing sections
        if microsoft:
            self.delete_different_timeframe(service='microsoft', project=project, speaker=speaker,
                                            performance_date=performance_date, speaker_type=speaker_type, part=part,
                                            timeframe=timeframe)
        if google:
            self.delete_different_timeframe(service='google', project=project, speaker=speaker,
                                            performance_date=performance_date, speaker_type=speaker_type, part=part,
                                            timeframe=timeframe)
        if ibm:
            self.delete_different_timeframe(service='ibm', project=project, speaker=speaker,
                                            performance_date=performance_date, speaker_type=speaker_type, part=part,
                                            timeframe=timeframe)
        if aws:
            self.delete_different_timeframe(service='aws', project=project, speaker=speaker,
                                            performance_date=performance_date, speaker_type=speaker_type, part=part,
                                            timeframe=timeframe)
        if self.repair_metadata:
            athena_db.query_athena_and_wait(query_string="MSCK REPAIR TABLE metadata")
            self.repair_metadata = False

        # create audio object and slice it according to timeframe (in hours)
        extension = Path(filepath).suffix[1:]
        sound = AudioSegment.from_file(filepath, extension)
        sound = sound.set_channels(1)
        if (timeframe * 60 * 60) > 13200.0:  # more than 3 hours and 40 minutes
            self.instance_type = 't3a.micro'
        chunk_length_ms = timeframe * 60 * 60 * 1000  # pydub calculates in millisec
        chunks = make_chunks(sound, chunk_length_ms)  # Make chunks of ten seconds

        # determine list of jobs that need to be performed
        jobs = list()
        for i in range(1, len(chunks)+1):
            if microsoft:
                jobs.append(
                    {
                        'project': project,
                        'speaker': speaker,
                        'performance_date': performance_date,
                        'speaker_type': speaker_type,
                        'part': part,
                        'timeframe': timeframe,
                        'section': i,
                        'service': 'microsoft'
                    }
                )
            if ibm:
                jobs.append(
                    {
                        'project': project,
                        'speaker': speaker,
                        'performance_date': performance_date,
                        'speaker_type': speaker_type,
                        'part': part,
                        'timeframe': timeframe,
                        'section': i,
                        'service': 'ibm'
                    }
                )
            if aws:
                jobs.append(
                    {
                        'project': project,
                        'speaker': speaker,
                        'performance_date': performance_date,
                        'speaker_type': speaker_type,
                        'part': part,
                        'timeframe': timeframe,
                        'section': i,
                        'service': 'aws'
                    }
                )
            if google:
                jobs.append(
                    {
                        'project': project,
                        'speaker': speaker,
                        'performance_date': performance_date,
                        'speaker_type': speaker_type,
                        'part': part,
                        'timeframe': timeframe,
                        'section': i,
                        'service': 'google'
                    }
                )
        jobs_values = ""
        for i in range(len(jobs)):
            jobs_values = f"('{jobs[i]['service']}','{jobs[i]['project']}','{jobs[i]['speaker']}'," \
                          f"'{jobs[i]['performance_date']}',{jobs[i]['part']},'{jobs[i]['speaker_type']}'," \
                          f"{jobs[i]['timeframe']},{jobs[i]['section']}),{jobs_values}"
        jobs_values = jobs_values[:-1] # eliminate the final comma
        jobs_athena = athena_db.query_athena_and_download(query_string=SELECT_JOBS.format(jobs_values=jobs_values),
                                                          filename='jobs.csv')
        with open(jobs_athena) as jobs_file:
            jobs_reader = csv.DictReader(jobs_file)
            jobs = list()
            for row in jobs_reader:
                jobs.append({
                    'service': row['service'],
                    'project': row['project'],
                    'speaker': row['speaker'],
                    'performance_date': row['performance_date'],
                    'part': row['part'],
                    'speaker_type': row['speaker_type'],
                    'timeframe': row['timeframe'],
                    'section': row['section']
                })

        # export audio e instantiate cloud transcribers
        if len(jobs) > 0:
            created_audio = False
            Path('./audio/').mkdir(parents=True, exist_ok=True)
            destination = list()
            try:
                # export audio
                for i, chunk in enumerate(chunks):
                    temp_sound_file = f"./audio/{uuid.uuid4()}.wav"
                    chunk.export(temp_sound_file, format="wav", parameters=['-acodec', 'pcm_s16le'])
                    created_audio = True
                    destination.append(temp_sound_file)
                # instantiate cloud transcribers
                for job in jobs:
                    self.instantiate_cloud_transcriber(service=job['service'],
                                                       project=job['project'],
                                                       performance_date=job['performance_date'],
                                                       part=job['part'],
                                                       timeframe=job['timeframe'],
                                                       section=job['section'],
                                                       language=language,
                                                       speaker=job['speaker'],
                                                       speaker_type=job['speaker_type'],
                                                       filepath=destination[int(job['section'])-1]) # destination is zero-based
            finally:
                if created_audio:
                    shutil.rmtree("./audio")

    def get_where_clause(self, project=None, speaker=None, performance_date=None, part=None):
        where_clause = ""
        if project is not None:
            where_clause = f"AND project = '{project}' "
        if speaker is not None:
            where_clause = f"{where_clause}AND speaker = '{speaker}' "
        if performance_date is not None:
            where_clause = f"{where_clause}AND performance_date = '{performance_date}' "
        if part is not None:
            where_clause = f"{where_clause}AND part = '{part}' "
        if where_clause != '':
            where_clause = f"where {where_clause[4:]}"
        return where_clause

    def add_timeframe_section_to_s3_path(self):
        athena_db = AthenaDatabase(database=self.config['aws']['athena'], s3_output=self.bucket)

        # metadata_records = athena_db.query_athena_and_download(query_string="select distinct service, project, "
        #                                                                     "speaker, performance_date, speaker_type "
        #                                                                     "from metadata "
        #                                                                     "order by service, project, speaker, "
        #                                                                     "performance_date, speaker_type",
        #                                                        filename='metadata_records.csv')
        # with open(metadata_records) as metadata_file:
        #     metadata_reader = csv.DictReader(metadata_file)
        #     for row in metadata_reader:
        #         print(f"{row['service']}/{row['project']}/{row['speaker']}/{row['performance_date']}/{row['speaker_type']}")
        #         move_data_in_s3(
        #             bucket_name=self.bucket,
        #             origin=f"transcript/service={row['service']}/project={row['project']}/speaker={row['speaker']}/performance_date={row['performance_date']}/part=1/timeframe=4/timesection=1/speaker_type={row['speaker_type']}/transcript.json.bz2",
        #             destination=f"transcript/service={row['service']}/project={row['project']}/speaker={row['speaker']}/performance_date={row['performance_date']}/part=1/speaker_type={row['speaker_type']}/timeframe=4/section=1/transcript.json.bz2"
        #         )

        athena_db.query_athena_and_wait(query_string="MSCK REPAIR TABLE word")
        words_records = athena_db.query_athena_and_download(query_string="select distinct project, speaker, "
                                                                         "performance_date, service, protagonist "
                                                                         "from word "
                                                                         "order by project, speaker, performance_date, "
                                                                         "service, protagonist",
                                                            filename='words_records.csv')
        with open(words_records) as words_file:
            words_reader = csv.DictReader(words_file)
            for row in words_reader:
                move_data_in_s3(
                    bucket_name=self.bucket,
                    origin=f"word/project={row['project']}/speaker={row['speaker']}/performance_date={row['performance_date']}/part=1/service={row['service']}/protagonist={row['protagonist']}/word.json.bz2",
                    destination=f"word/project={row['project']}/speaker={row['speaker']}/performance_date={row['performance_date']}/part=1/service={row['service']}/protagonist={row['protagonist']}/timeframe=4/section=1/word.json.bz2"
                )

    def parse_words(self, project=None, speaker=None, performance_date=None, part=None):
        # TODO: check if there are files lost in space on Microsoft and AWS especially.
        # TODO: see why it seems that the number of words on Athena is twice as it should be.
        athena_db = AthenaDatabase(database=self.config['aws']['athena'], s3_output=self.bucket)
        athena_db.query_athena_and_wait(query_string="MSCK REPAIR TABLE metadata")
        athena_db.query_athena_and_wait(query_string="MSCK REPAIR TABLE word")

        where_clause = self.get_where_clause(project=project, speaker=speaker, performance_date=performance_date, part=part)
        if where_clause == '':
            select = SELECT_NON_PARSED_TRANSCRIPTS.format(where_clause=where_clause).replace(' and ', ' where ', 1).replace('\n\n','\n')
        else:
            select = SELECT_NON_PARSED_TRANSCRIPTS.format(where_clause=where_clause)
        unparsed_records = athena_db.query_athena_and_download(query_string=select,
                                                               filename='unparsed_records.csv')
        with open(unparsed_records) as unparsed_file:
            reader = csv.DictReader(unparsed_file)
            database_has_changed = False
            try:
                print("Parse words...")
                for row in reader:
                    print(f"{row['speaker']}_{row['performance_date']}_{row['part']}_{row['service']}_{row['speaker_type']}_{row['section']}")
                    transcript = read_dict_from_s3(self.bucket,
                                                   f"transcript/service={row['service']}/project={row['project']}/speaker={row['speaker']}/"
                                                   f"performance_date={row['performance_date']}/part={row['part']}/"
                                                   f"speaker_type={row['speaker_type']}/timeframe={row['timeframe']}/"
                                                   f"section={row['section']}/transcript.json.bz2",
                                                   compressed=True)
                    protagonist_words, non_protagonist_words = parse_words(transcript=transcript,
                                                                           speaker_type=row['speaker_type'],
                                                                           service=row['service'])
                    partitions = OrderedDict()
                    partitions['project'] = row['project']
                    partitions['speaker'] = row['speaker']
                    partitions['performance_date'] = row['performance_date']
                    partitions['part'] = row['part']
                    partitions['service'] = row['service']
                    partitions['protagonist'] = -1
                    partitions['timeframe'] = row['timeframe']
                    partitions['section'] = row['section']
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

    def export_csv(self, project=None, speaker=None, performance_date=None, part=None, interval_in_seconds=10):
        self.parse_words(project=project, speaker=speaker, performance_date=performance_date, part=part)

        athena_db = AthenaDatabase(database=self.config['aws']['athena'], s3_output=self.bucket)
        where_clause = self.get_where_clause(project=project, speaker=speaker, performance_date=performance_date, part=part)
        tmp_file = athena_db.query_athena_and_download(
            query_string=SELECT_ALL_TRANSCRIPTS.format(where_clause=where_clause),
            filename='selected_transcripts.csv')
        with open(tmp_file) as csvfile:
            reader = csv.DictReader(csvfile)
            Path("./csv/").mkdir(parents=True, exist_ok=True)
            print("Export CSVs...")
            for row in reader:
                print(f"{row['project']}_{row['speaker']}_{row['performance_date']}_{row['part']}")

                filename = f"{row['project']}_{row['speaker']}_{row['performance_date']}_{row['part']}_{interval_in_seconds}.csv"
                new_file = athena_db.query_athena_and_download(SELECT_TRANSCRIPT.format(project=row['project'],
                                                                                        speaker=row['speaker'],
                                                                                        performance_date=row[
                                                                                            'performance_date'],
                                                                                        part=row['part'],
                                                                                        interval_in_seconds=interval_in_seconds),
                                                               filename)
                os.replace(new_file, f'./csv/{filename}')
