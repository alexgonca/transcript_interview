from ibm_watson import SpeechToTextV1
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
from time import sleep
import json
import boto3
import uuid
from pathlib import Path
import logging


def upload_audio_file(filepath, service_config):
    bucket_name = str(uuid.uuid4())
    location = {'LocationConstraint': service_config['aws_region']}
    s3_resource = boto3.resource('s3', region_name=service_config['aws_region'])
    bucket = s3_resource.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)

    extension = Path(filepath).suffix[1:]
    if extension == 'wav':
        media_object_key = "audio.wav"
    else:
        media_object_key = "audio.mp3"
    bucket.upload_file(filepath, media_object_key)
    return f"{bucket_name}/{media_object_key}"


def retrieve_transcript(identifier, language, speaker_type, service_config, phone=False):
    try:
        logging.info(f"Identifier outside delete_uploaded_file: {identifier}")
        s3_items = identifier.split('/')
        s3_resource = boto3.resource('s3')
        bucket = s3_resource.Bucket(s3_items[0])
        extension = Path(s3_items[1]).suffix[1:]
        if extension == 'wav':
            local_file = f"{uuid.uuid4()}.wav"
            content_type="audio/wav"
        else:
            local_file = f"{uuid.uuid4()}.mp3"
            content_type="audio/mp3"
        bucket.download_file(s3_items[1], local_file)
    finally:
        delete_uploaded_file(identifier=identifier, service_config=service_config)
    authenticator = IAMAuthenticator(service_config["api_key"])
    speech_to_text = SpeechToTextV1(authenticator=authenticator)

    speech_to_text.set_service_url(service_config["service_url"])

    if phone:
        model = f"{language}_NarrowbandModel"
    else:
        model = f"{language}_BroadbandModel"

    with open(local_file, 'rb') as audio_file:
        recognition_job = speech_to_text.create_job(
            audio_file,
            model=model,
            content_type=content_type,
            results_ttl=60,
            inactivity_timeout=-1,
            timestamps=True,
            speaker_labels=False, # right now, there is no diarization for Brazilian Portuguese
            word_confidence=True,
            profanity_filter=False
        ).get_result()

    while recognition_job['status'] in ('waiting', 'processing'):
        sleep(1000)
        recognition_job = speech_to_text.check_job(recognition_job['id']).get_result()

    if recognition_job['status'] == 'failed':
        raise Exception(json.dumps(recognition_job, indent=2))
    else:
        return recognition_job


def delete_uploaded_file(identifier, service_config):
    logging.info(f"Identifier inside delete_uploaded_file: {identifier}")
    s3_items = identifier.split('/')
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(s3_items[0])
    bucket.objects.delete()
    bucket.delete()

