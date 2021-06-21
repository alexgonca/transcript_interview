from pydub import AudioSegment
from pydub.utils import make_chunks
from pathlib import Path
import shutil
import json
import uuid
from internet_scholar import read_dict_from_s3, s3_prefix_exists, delete_s3_objects_by_prefix, save_data_in_s3, instantiate_ec2, AthenaDatabase, move_data_in_s3
from collections import OrderedDict
from transcriber_parser import parse_words
import csv
import os
from googleapiclient.discovery import build
from google.oauth2 import service_account

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

SELECT_ALL_PARTS = """select distinct performance_date, part
from word {where_clause} order by performance_date, part"""

SELECT_ALL_SPEAKERS = """select distinct speaker
from word {where_clause} order by speaker"""

SELECT_ALL_PROJECTS = """select distinct project
from word {where_clause} order by project"""

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
            self.repair_table_metadata()

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

        # self.repair_table_metadata()
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

        self.repair_table_word()
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

    def repair_table_metadata(self):
        print("Going to repair table metadata...")
        athena_db = AthenaDatabase(database=self.config['aws']['athena'], s3_output=self.bucket)
        athena_db.query_athena_and_wait(query_string="MSCK REPAIR TABLE metadata")
        self.repair_metadata = False
        print("Done.")

    def repair_table_word(self):
        print("Going to repair table word...")
        athena_db = AthenaDatabase(database=self.config['aws']['athena'], s3_output=self.bucket)
        athena_db.query_athena_and_wait(query_string="MSCK REPAIR TABLE word")
        print("Done.")

    def parse_words(self, project=None, speaker=None, performance_date=None, part=None):
        self.repair_table_metadata()
        self.repair_table_word()

        where_clause = self.get_where_clause(project=project, speaker=speaker, performance_date=performance_date, part=part)
        if where_clause == '':
            select = SELECT_NON_PARSED_TRANSCRIPTS.format(where_clause=where_clause).replace(' and ', ' where ', 1).replace('\n\n','\n')
        else:
            select = SELECT_NON_PARSED_TRANSCRIPTS.format(where_clause=where_clause)
        athena_db = AthenaDatabase(database=self.config['aws']['athena'], s3_output=self.bucket)
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
                    self.repair_table_word()

    def export_google_sheets(self, project=None, speaker=None, interval_in_seconds=10):
        self.parse_words(project=project, speaker=speaker)

        Path("./csv/").mkdir(parents=True, exist_ok=True)
        print("Export CSVs...")
        try:
            json_string = json.dumps(self.config['google'])
            Path('./local_credentials/').mkdir(parents=True, exist_ok=True)
            temp_file = f"./local_credentials/{uuid.uuid4()}.json"
            with open(temp_file, 'w', encoding="utf-8") as json_file:
                json_file.write(json_string)
            try:
                credentials_google_drive = service_account.Credentials.from_service_account_file(
                    temp_file,
                    scopes=['https://www.googleapis.com/auth/drive'])
                credentials_google_sheets = service_account.Credentials.from_service_account_file(
                    temp_file,
                    scopes=['https://www.googleapis.com/auth/spreadsheets'])
            finally:
                shutil.rmtree('./local_credentials')
            google_drive = build('drive', 'v3', credentials=credentials_google_drive)
            google_sheets = build('sheets', 'v4', credentials=credentials_google_sheets)

            athena_db = AthenaDatabase(database=self.config['aws']['athena'], s3_output=self.bucket)

            all_projects = athena_db.query_athena_and_download(
                query_string=SELECT_ALL_PROJECTS.format(where_clause=self.get_where_clause(project=project, speaker=speaker)),
                filename='selected_all_projects.csv')
            with open(all_projects) as all_projects_csv:
                projects_reader = csv.DictReader(all_projects_csv)
                for projects_row in projects_reader:
                    response_project = google_drive.files().list(
                        q=f"mimeType='application/vnd.google-apps.folder' and "
                          f"'{self.config['google']['transcription_folder']}' in parents and "
                          f"name='{projects_row['project']}'",
                        spaces='drive',
                        fields='files(id, name)').execute()
                    if len(response_project['files']) == 0:
                        folder_metadata = {
                            'name': projects_row['project'],
                            'mimeType': 'application/vnd.google-apps.folder',
                            'parents': [self.config['google']['transcription_folder'], ]
                        }
                        project_folder = google_drive.files().create(body=folder_metadata,
                                                                     fields='id').execute()
                        project_id = project_folder['id']
                    elif len(response_project['files']) == 1:
                        project_id = response_project['files'][0]['id']
                    else:
                        raise Exception("Error! Should not have more than 1 folder for this project!")

                    all_speakers = athena_db.query_athena_and_download(
                        query_string=SELECT_ALL_SPEAKERS.format(where_clause=self.get_where_clause(project=projects_row['project'], speaker=speaker)),
                        filename='selected_all_speakers.csv')
                    with open(all_speakers) as all_speakers_csv:
                        speakers_reader = csv.DictReader(all_speakers_csv)
                        for speakers_row in speakers_reader:
                            response_spreadsheet = google_drive.files().list(
                                q=f"mimeType='application/vnd.google-apps.spreadsheet' and '{project_id}' in parents and name='{speakers_row['speaker']}'",
                                spaces='drive',
                                fields='files(id, name)').execute()
                            if len(response_spreadsheet['files']) == 1:
                                print(f"Spreadsheet for {speakers_row['speaker']} already exists. I will not overwrite.")
                            elif len(response_spreadsheet['files']) >= 2:
                                raise Exception("Error! Should not have more than 1 spreadsheet for this project!")
                            else:  # it is 0
                                body = {
                                    'mimeType': 'application/vnd.google-apps.spreadsheet',
                                    'name': speakers_row['speaker'],
                                    'parents': [project_id, ]
                                }
                                response = google_drive.files().create(body=body, fields='id').execute()
                                speaker_id = response['id']
                                all_parts = athena_db.query_athena_and_download(
                                    query_string=SELECT_ALL_PARTS.format(
                                        where_clause=self.get_where_clause(project=projects_row['project'], speaker=speakers_row['speaker'])),
                                    filename='selected_all_parts.csv')
                                with open(all_parts) as all_parts_csv:
                                    parts_reader = csv.DictReader(all_parts_csv)
                                    first_sheet = True
                                    for parts_row in parts_reader:
                                        filename = f"{projects_row['project']}_{speakers_row['speaker']}_" \
                                                   f"{parts_row['performance_date']}_{parts_row['part']}_{interval_in_seconds}.csv"
                                        print(filename)
                                        new_file = athena_db.query_athena_and_download(SELECT_TRANSCRIPT.format(project=projects_row['project'],
                                                                                                                speaker=speakers_row['speaker'],
                                                                                                                performance_date=parts_row[
                                                                                                                    'performance_date'],
                                                                                                                part=parts_row['part'],
                                                                                                                interval_in_seconds=interval_in_seconds),
                                                                                       filename)
                                        os.replace(new_file, f'./csv/{filename}')
                                        if first_sheet:
                                            body = {
                                                'requests': {
                                                    "updateSheetProperties": {
                                                        "fields": "title,gridProperties.rowCount,gridProperties.columnCount,gridProperties.frozenRowCount",
                                                        "properties": {"title": f"{parts_row['performance_date']} / {parts_row['part']}",
                                                                       "gridProperties": {
                                                                           "rowCount": 3,
                                                                           "columnCount": 3,
                                                                           "frozenRowCount": 1
                                                                       },
                                                                       "index": 0}
                                                    }
                                                },
                                                'includeSpreadsheetInResponse': True
                                            }
                                            response = google_sheets.spreadsheets().batchUpdate(spreadsheetId=speaker_id,
                                                                                                body=body).execute()
                                            sheet_id = response['updatedSpreadsheet']['sheets'][0]['properties']['sheetId']
                                            first_sheet = False
                                        else:
                                            body = {
                                                "requests": {
                                                    "addSheet": {
                                                        "properties": {
                                                            "title": f"{parts_row['performance_date']} / {parts_row['part']}",
                                                            "gridProperties": {
                                                                "rowCount": 3,
                                                                "columnCount": 3,
                                                                "frozenRowCount": 1
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                            response = google_sheets.spreadsheets().batchUpdate(spreadsheetId=speaker_id,
                                                                                                body=body).execute()
                                            sheet_id = response['replies'][0]['addSheet']['properties']['sheetId']
                                        with open(f'./csv/{filename}', 'r', encoding="utf-8") as csv_file:
                                            csvContents = csv_file.read()
                                        body = {
                                            'requests': [{
                                                'pasteData': {
                                                    "coordinate": {
                                                        "sheetId": sheet_id,
                                                        "rowIndex": "0",  # adapt this if you need different positioning
                                                        "columnIndex": "0",  # adapt this if you need different positioning
                                                    },
                                                    "data": csvContents,
                                                    "type": 'PASTE_NORMAL',
                                                    "delimiter": ',',
                                                }
                                            }]
                                        }
                                        response = google_sheets.spreadsheets().batchUpdate(spreadsheetId=speaker_id,
                                                                                            body=body).execute()
                                        body = {
                                            "requests": [
                                                {
                                                    "repeatCell": {
                                                        "range": {
                                                            "sheetId": sheet_id,
                                                            "startRowIndex": 0,
                                                            "startColumnIndex": 0
                                                        },
                                                        "cell":
                                                            {
                                                                "userEnteredFormat": {
                                                                    "verticalAlignment": "TOP",
                                                                    "wrapStrategy": "WRAP"
                                                                },
                                                            },
                                                        "fields": "userEnteredFormat.wrapStrategy,userEnteredFormat.verticalAlignment"
                                                    }
                                                },
                                                {
                                                    "updateDimensionProperties": {
                                                        "range": {
                                                            "sheetId": sheet_id,
                                                            "dimension": "COLUMNS",
                                                            "startIndex": 0,
                                                            "endIndex": 1
                                                        },
                                                        "properties": {
                                                            "pixelSize": 60
                                                        },
                                                        "fields": "pixelSize"
                                                    }
                                                },
                                                {
                                                    "updateDimensionProperties": {
                                                        "range": {
                                                            "sheetId": sheet_id,
                                                            "dimension": "COLUMNS",
                                                            "startIndex": 1
                                                        },
                                                        "properties": {
                                                            "pixelSize": 280
                                                        },
                                                        "fields": "pixelSize"
                                                    }
                                                }
                                            ]
                                        }
                                        response = google_sheets.spreadsheets().batchUpdate(spreadsheetId=speaker_id,
                                                                                            body=body).execute()
        finally:
            shutil.rmtree('./csv')
