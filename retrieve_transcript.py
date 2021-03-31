from pydub import AudioSegment
import sqlite3
from pathlib import Path
import shutil
import bz2
import google_transcribe
import microsoft_transcribe
import ibm_transcribe
import aws_transcribe
import json
import configparser
import traceback
import csv
from datetime import timedelta
import boto3
import botocore
import uuid
import os


def decompress(filename, delete_original=True):
    new_filepath = filename[:-4]
    with bz2.BZ2File(filename, 'rb') as input_file:
        with open(new_filepath, 'wb') as output_file:
            shutil.copyfileobj(input_file, output_file)
    if delete_original:
        os.remove(filename)
    return new_filepath


def compress(filename, delete_original=True, compress_level=9):
    filename_bz2 = Path(Path(__file__).parent, 'tmp', "{}.bz2".format(filename))
    print(f"Compress file {filename}.")
    with open(filename, 'rb') as input_file:
        with bz2.BZ2File(filename_bz2, 'wb', compresslevel=compress_level) as output_file:
            shutil.copyfileobj(input_file, output_file)
    if delete_original:
        os.remove(filename)
    return filename_bz2


class Transcript:
    INTERVIEWEE = "interviewee"
    INTERVIEWER = "interviewer"
    BOTH = "both"

    __CREATE_TABLE_INTERVIEW = """
    create table if not exists interview
        (label TEXT,
        speaker TEXT,
        service TEXT,
        config TEXT,
        transcript TEXT,
        created_at TEXT,
        primary key (label, speaker, service))
    """

    __INSERT_INTERVIEW = """
    insert into interview
    (label, speaker, service, config, transcript, created_at)
    VALUES
    (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    """

    __CREATE_TABLE_ERROR = """
    create table if not exists error
        (label TEXT,
        speaker TEXT,
        service TEXT,
        config TEXT,
        message TEXT,
        created_at TEXT)
    """

    __INSERT_ERROR = """
    insert into error
    (label, speaker, service, config, message, created_at)
    VALUES
    (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    """

    __CREATE_TABLE_WORD = """
    create table if not exists word
        (label TEXT,
        service TEXT,
        word TEXT,
        start_time INTEGER,
        end_time INTEGER,
        interviewee INTEGER)
    """

    __INSERT_WORD = """
    insert into word
    (label, service, word, start_time, end_time, interviewee)
    VALUES
    (?, ?, ?, ?, ?, ?)
    """

    __SELECT_WORD = """
    SELECT word, interviewee
    from word
    WHERE label = ? and service = ? and start_time >= ? and start_time < ?
    order by start_time
    """

    def __init__(self):
        self.changed_database = False
        print("Download database if exists!")
        self.db_name = Path(Path(__file__).parent, 'db', 'interviews.sqlite')
        Path(self.db_name).parent.mkdir(parents=True, exist_ok=True)

        config_file = configparser.ConfigParser()
        config_file.read('config.ini')
        session = boto3.Session(
            aws_access_key_id=config_file['aws']['access_key'],
            aws_secret_access_key=config_file['aws']['secret_key'],
            region_name=config_file['aws']['region']
        )
        s3_resource = session.resource('s3')
        bucket = s3_resource.Bucket(config_file['aws']['s3_bucket'])
        try:
            bucket.download_file('database/interviews.sqlite.bz2', str(self.db_name)+".bz2")
            decompress(str(self.db_name)+".bz2")
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                database = sqlite3.connect(str(self.db_name), isolation_level=None)
                database.execute(self.__CREATE_TABLE_INTERVIEW)
                database.execute(self.__CREATE_TABLE_ERROR)
                database.execute(self.__CREATE_TABLE_WORD)
                database.close()
            else:
                raise

    def inner_retrieve_transcript(self, config, filepath, service):
        database = sqlite3.connect(str(self.db_name))
        try:
            config_file = configparser.ConfigParser()
            config_file.read('config.ini')

            cursor = database.cursor()
            cursor.execute("SELECT transcript FROM interview WHERE label = ? and speaker = ? and service = ?",
                           (config['label'], config['speaker'], service))
            data = cursor.fetchone()

            if data is not None:
                print('Data was already retrieved from service {service}. Using transcript from database.'.format(service=service))
                transcript = json.loads(data[0])
            else:
                print(f"Begin conversion for {service}.")
                if service == "microsoft":
                    transcript = microsoft_transcribe.retrieve_transcript(filepath=filepath,
                                                                          language=config['language'],
                                                                          speaker=config['speaker'],
                                                                          account_name=config_file['microsoft']['account_name'],
                                                                          account_key=config_file['microsoft']['account_key'],
                                                                          subscription_key=config_file['microsoft']['subscription_key'],
                                                                          connection_string=config_file['microsoft']['connection_string'],
                                                                          service_region=config_file['microsoft']['service_region'])
                elif service == "google":
                    transcript = google_transcribe.retrieve_transcript(path_config=config_file['google']['config_json'],
                                                                       bucket_name=config_file['google']['bucket'],
                                                                       filepath=filepath,
                                                                       language=config['language'],
                                                                       speaker=config['speaker'])
                elif service == "ibm":
                    transcript = ibm_transcribe.retrieve_transcript(filepath=filepath,
                                                                    language=config['language'],
                                                                    api_key=config_file['ibm']['api_key'],
                                                                    service_url=config_file['ibm']['service_url'])
                elif service == "aws":
                    transcript = aws_transcribe.retrieve_transcript(filepath=filepath,
                                                                    language=config['language'],
                                                                    speaker=config['speaker'],
                                                                    access_key=config_file['aws']['access_key'],
                                                                    secret_key=config_file['aws']['secret_key'],
                                                                    region=config_file['aws']['region'])
                else:
                    raise NameError('Service not defined!')

                database.execute(self.__INSERT_INTERVIEW, (config['label'],
                                                           config['speaker'],
                                                           service,
                                                           json.dumps(config),
                                                           json.dumps(transcript)))
                database.execute('COMMIT')
                self.changed_database = True
                print('Success transcript! {service}'.format(service=service))

            if config['speaker'] == self.BOTH:
                cursor.execute("SELECT rowid FROM word WHERE label = ? and service = ?",
                               (config['label'], service))
            elif config['speaker'] == self.INTERVIEWEE:
                cursor.execute("SELECT rowid FROM word WHERE label = ? and service = ? and interviewee = ?",
                               (config['label'], service, 1))
            elif config['speaker'] == self.INTERVIEWER:
                cursor.execute("SELECT rowid FROM word WHERE label = ? and service = ? and interviewee = ?",
                               (config['label'], service, 0))
            else:
                print('Invalid config[speaker].')

            data = cursor.fetchone()
            if data is not None:
                print('Words were already extracted from this service {service}. Ignoring.'.format(service=service))
            else:
                if service == "microsoft":
                    words = microsoft_transcribe.parse_words(transcript, speaker=config['speaker'])
                elif service == "google":
                    words = google_transcribe.parse_words(transcript, speaker=config['speaker'])
                elif service == "ibm":
                    words = ibm_transcribe.parse_words(transcript, speaker=config['speaker'])
                elif service == "aws":
                    words = aws_transcribe.parse_words(transcript, speaker=config['speaker'])
                else:
                    raise NameError('Service {service} not defined!'.format(service=service))
                if len(words) > 0:
                    for word in words:
                        database.execute(self.__INSERT_WORD, (config['label'],
                                                              service,
                                                              word['word'],
                                                              word['start_time'],
                                                              word['end_time'],
                                                              word['interviewee']))
                    database.execute('COMMIT')
                    self.changed_database = True
                    print('Success parsing! {service}'.format(service=service))
        except:
            print('Error! {service}'.format(service=service))
            database.execute(self.__INSERT_ERROR, (config['label'],
                                                   config['speaker'],
                                                   service,
                                                   json.dumps(config),
                                                   traceback.format_exc()))
            database.execute('COMMIT')
            self.changed_database = True
        finally:
            database.close()

    def retrieve_transcript(self, config, microsoft=False, google=False, ibm=False, aws=False):
        print("Download file")
        config_file = configparser.ConfigParser()
        config_file.read('config.ini')
        session = boto3.Session(
            aws_access_key_id=config_file['aws']['access_key'],
            aws_secret_access_key=config_file['aws']['secret_key'],
            region_name=config_file['aws']['region']
        )
        s3_resource = session.resource('s3')
        bucket = s3_resource.Bucket(config_file['aws']['s3_bucket'])
        extension = Path(config['s3_key']).suffix[1:]
        temp_file = Path(f"./audio/{uuid.uuid4()}.{extension}")
        Path("./audio").mkdir(parents=True, exist_ok=True)
        bucket.download_file(config['s3_key'], str(temp_file))

        try:
            print('Converting audio to WAV.')
            sound = AudioSegment.from_file(str(temp_file), extension)
            sound = sound.set_channels(1)
            destination = "./audio/{label}_{speaker}.wav".format(label=config['label'], speaker=config['speaker'])
            sound.export(destination, format="wav")
            print('Finish converting.')

            if microsoft:
                self.inner_retrieve_transcript(config=config, filepath=destination, service="microsoft")
            if google:
                self.inner_retrieve_transcript(config=config, filepath=destination, service="google")
            if ibm:
                self.inner_retrieve_transcript(config=config, filepath=destination, service="ibm")
            if aws:
                self.inner_retrieve_transcript(config=config, filepath=destination, service="aws")
        finally:
            shutil.rmtree("./audio")

    def export_csv(self, label, interval_in_milliseconds=5000):
        print(f"Export CSV: {label}")
        database = sqlite3.connect(str(self.db_name))

        filename = Path(Path(__file__).parent, 'csv', f'{label}-{interval_in_milliseconds}.csv')
        Path(filename).parent.mkdir(parents=True, exist_ok=True)

        cursor = database.cursor()
        cursor.execute("SELECT distinct service from word WHERE label = ?", (label,))
        services = []
        records = cursor.fetchall()
        for row in records:
            services.append(row[0])

        cursor.execute("SELECT max(start_time) from word WHERE label = ?", (label,))
        data = cursor.fetchone()
        max_time = data[0]

        with open(str(filename), 'w', newline='', encoding='utf8') as csvfile:
            fieldnames = ['start_time'] + services
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            start_time = 0
            while start_time <= max_time:
                new_row = {'start_time': str(timedelta(milliseconds=start_time))}
                end_time = start_time + interval_in_milliseconds
                for service in services:
                    cursor.execute(self.__SELECT_WORD,
                                   (label, service, start_time, end_time))
                    phrase = []
                    words = cursor.fetchall()
                    for word in words:
                        if word[1] == 1:
                            phrase.append(word[0])
                        else:
                            phrase.append(word[0].upper())
                    new_row[service] = ' '.join(phrase)
                start_time = end_time
                writer.writerow(new_row)

        database.close()
        print("Exported!")

    def upload_database(self):
        if self.changed_database:
            print("Upload database!")
            compress(str(self.db_name))
            config_file = configparser.ConfigParser()
            config_file.read('config.ini')
            session = boto3.Session(
                aws_access_key_id=config_file['aws']['access_key'],
                aws_secret_access_key=config_file['aws']['secret_key'],
                region_name=config_file['aws']['region']
            )
            s3_resource = session.resource('s3')
            bucket = s3_resource.Bucket(config_file['aws']['s3_bucket'])
            bucket.upload_file(str(self.db_name)+".bz2", 'database/interviews.sqlite.bz2')
        shutil.rmtree("./db")