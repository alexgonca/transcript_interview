from google.cloud import storage
from google.cloud import speech_v1p1beta1 as speech
from google.protobuf.json_format import MessageToDict
from pathlib import Path
import uuid
import shutil
import json


def upload_audio_file(filepath, service_config):
    storage_client = get_google_client(type="storage", service_config=service_config)
    bucket_name = str(uuid.uuid4())
    bucket = storage_client.create_bucket(bucket_name, location="us")
    blob = bucket.blob("audio.wav")
    storage.blob._DEFAULT_CHUNKSIZE = 2097152 # 1024 * 1024 B * 2 = 2 MB
    storage.blob._MAX_MULTIPART_SIZE = 2097152 # 2 MB
    blob.upload_from_filename(filepath)
    return bucket_name


def retrieve_transcript(identifier, language, speaker_type, service_config):
    gcs_uri = f"gs://{identifier}/audio.wav"
    audio = speech.RecognitionAudio(uri=gcs_uri)

    if speaker_type == 'both':
        recognition_config = speech.RecognitionConfig(
            enable_automatic_punctuation=True,
            enable_word_time_offsets=True,
            enable_speaker_diarization=True,
            diarization_speaker_count=2,
            language_code=language
        )
    elif speaker_type in ['interviewee', 'interviewer']:
        recognition_config = speech.RecognitionConfig(
            enable_automatic_punctuation=True,
            enable_word_time_offsets=True,
            enable_speaker_diarization=False,
            language_code=language
        )
    else:
        raise TypeError('unknown speaker type: {speaker}'.format(speaker=speaker_type))
    speech_client = get_google_client(type="speech", service_config=service_config)
    operation = speech_client.long_running_recognize(config=recognition_config, audio=audio)
    response = operation.result()
    response_dict = MessageToDict(response.__class__.pb(response))
    return response_dict


def get_google_client(type, service_config):
    json_string = json.dumps(service_config)
    Path('./local_credentials/').mkdir(parents=True, exist_ok=True)
    temp_file = f"./local_credentials/{uuid.uuid4()}.json"
    with open(temp_file, 'w', encoding="utf-8") as json_file:
        json_file.write(json_string)
    if type == "storage":
        client = storage.Client.from_service_account_json(temp_file)
    elif type == "speech":
        client = speech.SpeechClient.from_service_account_json(temp_file)
    else:
        client = None
    shutil.rmtree('./local_credentials')
    return client


def delete_uploaded_file(identifier, service_config):
    storage_client = get_google_client(type="storage", service_config=service_config)
    bucket = storage_client.get_bucket(identifier)
    bucket.delete(force=True)
