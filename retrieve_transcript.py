from pydub import AudioSegment
from pathlib import Path
import shutil
import botocore
import uuid
from internet_scholar import read_dict_from_s3, s3_key_exists, save_data_in_s3, instantiate_ec2
from collections import OrderedDict
from parse_words import parse_words


INIT_SCRIPT = """#!/bin/bash
sudo apt update && \
sudo apt install -y python3-pip unzip && \
wget https://github.com/alexgonca/transcript_interview/archive/refs/heads/main.zip && \
unzip main.zip && \
rm main.zip && \
find ./transcript_interview-main/* -maxdepth 0 -type d,f -exec mv -t ./ {{}} + && \
rm -R ./transcript_interview-main && \
wget https://raw.githubusercontent.com/internet-scholar/internet_scholar/master/requirements.txt -O requirements2.txt && \
wget https://raw.githubusercontent.com/internet-scholar/internet_scholar/master/internet_scholar.py && \
pip3 install --trusted-host pypi.python.org -r /home/ubuntu/requirements.txt && \
pip3 install --trusted-host pypi.python.org -r /home/ubuntu/requirements2.txt && \
python3 cloud_transcriber.py -b {bucket} -i {identifier} -l {language} -s {speaker} -t {speaker_type} -d {performance_date} -p {project} -v {service}
sudo shutdown -h now
"""


class Transcript:
    def __init__(self, bucket):
        self.bucket = bucket
        self.config = read_dict_from_s3(bucket=self.bucket, key='config/config.json')

    def check_existing_data(self, project, speaker, performance_date, speaker_type, service, retrieved, parsed):
        try:
            retrieved[service] = read_dict_from_s3(self.bucket,
                                                   f'transcript/service={service}/project={project}/speaker={speaker}/'
                                                   f'performance_date={performance_date}/speaker_type={speaker_type}/transcript.json.bz2',
                                                   compressed=True)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "NoSuchKey":
                retrieved[service] = None
            else:
                raise
        else:
            if speaker_type in ('single', 'interviewee'):
                parsed[service] = s3_key_exists(self.bucket,
                                                f'word/project={project}/speaker={speaker}/performance_date={performance_date}/'
                                                f'service={service}/protagonist=1/word.json.bz2')
            elif speaker_type == 'interviewer':
                parsed[service] = s3_key_exists(self.bucket,
                                                f'word/project={project}/speaker={speaker}/performance_date={performance_date}/'
                                                f'service={service}/protagonist=0/word.json.bz2')
            elif speaker_type == 'both':
                parsed[service] = s3_key_exists(self.bucket,
                                                f'word/project={project}/speaker={speaker}/performance_date={performance_date}/'
                                                f'service={service}/protagonist=1/word.json.bz2') or \
                                  s3_key_exists(self.bucket,
                                                f'word/project={project}/speaker={speaker}/performance_date={performance_date}/'
                                                f'service={service}/protagonist=0/word.json.bz2')
            else:
                raise TypeError("Unknown speaker type")

    # todo prevent two competing servers
    # todo split requirements.txt in several files
    # todo create tables for each service in Athena
    # todo import existing data

    def instantiate_cloud_transcriber(self, service, retrieved, project, performance_date,
                                      parsed, language, speaker, speaker_type, filepath, original_file=None):
        size = 8
        if service == "microsoft":
            from microsoft_transcribe import upload_audio_file, delete_uploaded_file
        elif service == "google":
            from google_transcribe import upload_audio_file, delete_uploaded_file
        elif service == "aws":
            from aws_transcribe import upload_audio_file, delete_uploaded_file
        elif service == "ibm":
            from ibm_transcribe import upload_audio_file, delete_uploaded_file
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

        if not retrieved[service]:
            identifier = upload_audio_file(filepath=filepath, service_config=self.config[service])
            try:
                instantiate_ec2(ami=self.config['aws']['ami'],
                                key_name=self.config['aws']['key_name'],
                                security_group=self.config['aws']['security_group'],
                                iam=self.config['aws']['iam'],
                                instance_type='t3a.nano',
                                size=size,
                                init_script=INIT_SCRIPT.format(bucket=self.bucket,
                                                               identifier=identifier,
                                                               language=language,
                                                               speaker=speaker,
                                                               speaker_type=speaker_type,
                                                               performance_date=performance_date,
                                                               project=project,
                                                               service=service),
                                name=f"{service}_transcribe",
                                simulation=False)
            except:
                delete_uploaded_file(identifier=identifier, service_config=self.config[service])
                raise
        elif not parsed[service]:
            protagonist_words, non_protagonist_words = parse_words(transcript=retrieved[service],
                                                                   speaker_type=speaker_type,
                                                                   service=service)
            partitions = OrderedDict()
            partitions['project'] = project
            partitions['speaker'] = speaker
            partitions['performance_date'] = performance_date
            partitions['service'] = service
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

    def retrieve_transcript(self, project, speaker, performance_date,
                            filepath, speaker_type, language,
                            microsoft=False, ibm=False, aws=False, google=False):
        retrieved = {
            'microsoft': None,
            'google': None,
            'aws': None,
            'ibm': None
        }
        parsed = {
            'microsoft': None,
            'google': None,
            'aws': None,
            'ibm': None
        }
        if microsoft:
            self.check_existing_data(project=project, speaker=speaker, performance_date=performance_date,
                                     speaker_type=speaker_type, service='microsoft',
                                     retrieved=retrieved, parsed=parsed)
        if google:
            self.check_existing_data(project=project, speaker=speaker, performance_date=performance_date,
                                     speaker_type=speaker_type, service='google',
                                     retrieved=retrieved, parsed=parsed)
        if aws:
            self.check_existing_data(project=project, speaker=speaker, performance_date=performance_date,
                                     speaker_type=speaker_type, service='aws',
                                     retrieved=retrieved, parsed=parsed)
        if ibm:
            self.check_existing_data(project=project, speaker=speaker, performance_date=performance_date,
                                     speaker_type=speaker_type, service='ibm',
                                     retrieved=retrieved, parsed=parsed)

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
            if microsoft:
                self.instantiate_cloud_transcriber(service="microsoft",
                                                   retrieved=retrieved,
                                                   project=project,
                                                   performance_date=performance_date,
                                                   parsed=parsed,
                                                   language=language,
                                                   speaker=speaker,
                                                   speaker_type=speaker_type,
                                                   filepath=destination)
            if google:
                self.instantiate_cloud_transcriber(service="google",
                                                   retrieved=retrieved,
                                                   project=project,
                                                   performance_date=performance_date,
                                                   parsed=parsed,
                                                   language=language,
                                                   speaker=speaker,
                                                   speaker_type=speaker_type,
                                                   filepath=destination)
            if ibm:
                self.instantiate_cloud_transcriber(service="ibm",
                                                   retrieved=retrieved,
                                                   project=project,
                                                   performance_date=performance_date,
                                                   parsed=parsed,
                                                   language=language,
                                                   speaker=speaker,
                                                   speaker_type=speaker_type,
                                                   filepath=destination,
                                                   original_file=filepath)
            if aws:
                self.instantiate_cloud_transcriber(service="aws",
                                                   retrieved=retrieved,
                                                   project=project,
                                                   performance_date=performance_date,
                                                   parsed=parsed,
                                                   language=language,
                                                   speaker=speaker,
                                                   speaker_type=speaker_type,
                                                   filepath=destination)
        finally:
            if created_audio:
                shutil.rmtree("./audio")
