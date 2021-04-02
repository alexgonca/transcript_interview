from google.cloud import storage
from google.cloud import speech_v1p1beta1 as speech
from google.protobuf.json_format import MessageToDict
from pathlib import Path
import uuid
import shutil
import json


def upload_audio_file(filepath, service_config):
    json_string = json.dumps(service_config)
    Path('./tmp/').mkdir(parents=True, exist_ok=True)
    temp_file = f"./tmp/{uuid.uuid4()}.json"
    with open(temp_file, 'w', encoding="utf-8") as json_file:
        json_file.write(json_string)
    try:
        storage_client = storage.Client.from_service_account_json(temp_file)
        bucket_name = str(uuid.uuid4())
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob("audio.wav")
        blob.upload_from_filename(filepath)
    finally:
        shutil.rmtree('./tmp')
    return bucket_name


def retrieve_transcript(identifier, language, speaker_type, service_config):
    json_string = json.dumps(service_config)
    Path('./tmp/').mkdir(parents=True, exist_ok=True)
    temp_file = f"./tmp/{uuid.uuid4()}.json"
    with open(temp_file, 'w', encoding="utf-8") as json_file:
        json_file.write(json_string)
    try:
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
        speech_client = speech.SpeechClient.from_service_account_json(temp_file)
        operation = speech_client.long_running_recognize(config=recognition_config, audio=audio)
        response = operation.result()
        response_dict = MessageToDict(response.__class__.pb(response))
    finally:
        storage_client = storage.Client.from_service_account_json(temp_file)
        bucket = storage_client.bucket(identifier)
        blob = bucket.blob("audio.wav")
        blob.delete()
        shutil.rmtree('./tmp')
    return response_dict