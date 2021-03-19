from google.cloud import storage
from google.cloud import speech_v1p1beta1 as speech
from google.protobuf.json_format import MessageToDict
import uuid


def retrieve_transcript(path_config, bucket_name, filepath, language, speaker):
    storage_client = storage.Client.from_service_account_json(path_config)
    bucket = storage_client.bucket(bucket_name)
    temp_name = "{temp_name}.wav".format(temp_name=str(uuid.uuid4()))
    blob = bucket.blob(temp_name)
    blob.upload_from_filename(filepath)

    try:
        gcs_uri = "gs://{bucket_name}/{temp_name}".format(bucket_name=bucket_name, temp_name=temp_name)
        audio = speech.RecognitionAudio(uri=gcs_uri)

        if speaker == 'both':
            recognition_config = speech.RecognitionConfig(
                enable_automatic_punctuation=True,
                enable_word_time_offsets=True,
                enable_speaker_diarization=True,
                diarization_speaker_count=2,
                language_code=language
            )
        elif speaker in ['interviewee', 'interviewer']:
            recognition_config = speech.RecognitionConfig(
                enable_automatic_punctuation=True,
                enable_word_time_offsets=True,
                enable_speaker_diarization=False,
                language_code=language
            )
        else:
            raise TypeError('unknown speaker type: {speaker}'.format(speaker=speaker))
        speech_client = speech.SpeechClient.from_service_account_json(path_config)
        operation = speech_client.long_running_recognize(config=recognition_config, audio=audio)
        response = operation.result()
        response_dict = MessageToDict(response.__class__.pb(response))
        return response_dict
    finally:
        blob.delete()


def parse_words(transcript, speaker):
    words = []
    if speaker == "both":
        for word in transcript['results'][-1]['alternatives'][0]['words']:
            if word['speakerTag'] == 1:
                interviewee = 0
            else:
                interviewee = 1
            words.append({
                'service': 'google',
                'word': word['word'],
                'start_time': int(float(word['startTime'][:-1]) * 1000),
                'end_time': int(float(word['endTime'][:-1]) * 1000),
                'interviewee': interviewee
            })
    elif speaker in ['interviewee', 'interviewer']:
        if speaker == 'interviewee':
            interviewee = 1
        else:
            interviewee = 0
        for word_cluster in transcript['results']:
            for word in word_cluster['alternatives'][0]['words']:
                words.append({
                    'service': 'google',
                    'word': word['word'],
                    'start_time': int(float(word['startTime'][:-1]) * 1000),
                    'end_time': int(float(word['endTime'][:-1]) * 1000),
                    'interviewee': interviewee
                })
    else:
        raise TypeError('Unknown speaker type: {speaker}'.format(speaker=speaker))
    return words