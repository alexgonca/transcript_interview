from ibm_watson import SpeechToTextV1
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
from time import sleep
import json


def retrieve_transcript(filepath, language, api_key, service_url, phone=False):
    authenticator = IAMAuthenticator(api_key)
    speech_to_text = SpeechToTextV1(authenticator=authenticator)

    speech_to_text.set_service_url(service_url)

    if phone:
        model = f"{language}_NarrowbandModel"
    else:
        model = f"{language}_BroadbandModel"

    with open(filepath, 'rb') as audio_file:
        recognition_job = speech_to_text.create_job(
            audio_file,
            model=model,
            content_type='audio/wav',
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


def parse_words(transcript, speaker):
    words = []
    if speaker in ("interviewee", "both"):
        interviewee = 1
    else:
        interviewee = 0
    for outer_result in transcript['results']:
        for inner_result in outer_result['results']:
            for word in inner_result['alternatives'][0]['timestamps']:
                words.append({
                    'service': 'ibm',
                    'word': word[0],
                    'start_time': int(word[1] * 1000),
                    'end_time': int(word[2] * 1000),
                    'interviewee': interviewee
                })
    return words
