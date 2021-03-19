import boto3
import time
import uuid
from botocore.exceptions import ClientError
from enum import Enum
import logging
import botocore.waiter
import requests


logger = logging.getLogger(__name__)


class WaitState(Enum):
    SUCCESS = 'success'
    FAILURE = 'failure'


class CustomWaiter:
    """
    Base class for a custom waiter that leverages botocore's waiter code. Waiters
    poll an operation, with a specified delay between each polling attempt, until
    either an accepted result is returned or the number of maximum attempts is reached.

    To use, implement a subclass that passes the specific operation, arguments,
    and acceptors to the superclass.

    For example, to implement a custom waiter for the transcription client that
    waits for both success and failure outcomes of the get_transcription_job function,
    create a class like the following:

        class TranscribeCompleteWaiter(CustomWaiter):
        def __init__(self, client):
            super().__init__(
                'TranscribeComplete', 'GetTranscriptionJob',
                'TranscriptionJob.TranscriptionJobStatus',
                {'COMPLETED': WaitState.SUCCESS, 'FAILED': WaitState.FAILURE},
                client)

        def wait(self, job_name):
            self._wait(TranscriptionJobName=job_name)

    """
    def __init__(
            self, name, operation, argument, acceptors, client, delay=10, max_tries=60,
            matcher='path'):
        """
        Subclasses should pass specific operations, arguments, and acceptors to
        their superclass.

        :param name: The name of the waiter. This can be any descriptive string.
        :param operation: The operation to wait for. This must match the casing of
                          the underlying operation model, which is typically in
                          CamelCase.
        :param argument: The dict keys used to access the result of the operation, in
                         dot notation. For example, 'Job.Status' will access
                         result['Job']['Status'].
        :param acceptors: The list of acceptors that indicate the wait is over. These
                          can indicate either success or failure. The acceptor values
                          are compared to the result of the operation after the
                          argument keys are applied.
        :param client: The Boto3 client.
        :param delay: The number of seconds to wait between each call to the operation.
        :param max_tries: The maximum number of tries before exiting.
        :param matcher: The kind of matcher to use.
        """
        self.name = name
        self.operation = operation
        self.argument = argument
        self.client = client
        self.waiter_model = botocore.waiter.WaiterModel({
            'version': 2,
            'waiters': {
                name: {
                    "delay": delay,
                    "operation": operation,
                    "maxAttempts": max_tries,
                    "acceptors": [{
                        "state": state.value,
                        "matcher": matcher,
                        "argument": argument,
                        "expected": expected
                    } for expected, state in acceptors.items()]
                }}})
        self.waiter = botocore.waiter.create_waiter_with_client(
            self.name, self.waiter_model, self.client)

    def __call__(self, parsed, **kwargs):
        """
        Handles the after-call event by logging information about the operation and its
        result.

        :param parsed: The parsed response from polling the operation.
        :param kwargs: Not used, but expected by the caller.
        """
        status = parsed
        for key in self.argument.split('.'):
            if key.endswith('[]'):
                status = status.get(key[:-2])[0]
            else:
                status = status.get(key)
        logger.info(
            "Waiter %s called %s, got %s.", self.name, self.operation, status)

    def _wait(self, **kwargs):
        """
        Registers for the after-call event and starts the botocore wait loop.

        :param kwargs: Keyword arguments that are passed to the operation being polled.
        """
        event_name = f'after-call.{self.client.meta.service_model.service_name}'
        self.client.meta.events.register(event_name, self)
        self.waiter.wait(**kwargs)
        self.client.meta.events.unregister(event_name, self)


class TranscribeCompleteWaiter(CustomWaiter):
    """
    Waits for the transcription to complete.
    """
    def __init__(self, client):
        super().__init__(
            'TranscribeComplete', 'GetTranscriptionJob',
            'TranscriptionJob.TranscriptionJobStatus',
            {'COMPLETED': WaitState.SUCCESS, 'FAILED': WaitState.FAILURE},
            client)

    def wait(self, job_name):
        self._wait(TranscriptionJobName=job_name)


def start_job(
        job_name, media_uri, media_format, language_code, speaker, transcribe_client,
        vocabulary_name=None):
    """
    Starts a transcription job. This function returns as soon as the job is started.
    To get the current status of the job, call get_transcription_job. The job is
    successfully completed when the job status is 'COMPLETED'.

    :param job_name: The name of the transcription job. This must be unique for
                     your AWS account.
    :param media_uri: The URI where the audio file is stored. This is typically
                      in an Amazon S3 bucket.
    :param media_format: The format of the audio file. For example, mp3 or wav.
    :param language_code: The language code of the audio file.
                          For example, en-US or ja-JP
    :param transcribe_client: The Boto3 Transcribe client.
    :param vocabulary_name: The name of a custom vocabulary to use when transcribing
                            the audio file.
    :return: Data about the job.
    """
    try:
        job_args = {
            'TranscriptionJobName': job_name,
            'Media': {'MediaFileUri': media_uri},
            'MediaFormat': media_format,
            'LanguageCode': language_code,
            'Settings': {
                'ShowAlternatives': False,
                'ShowSpeakerLabels': False
            }}
        if speaker == "both":
            job_args['Settings']['ShowSpeakerLabels'] = True
            job_args['Settings']['MaxSpeakerLabels'] = 2
        if vocabulary_name is not None:
            job_args['Settings']['VocabularyName'] = vocabulary_name
        response = transcribe_client.start_transcription_job(**job_args)
        job = response['TranscriptionJob']
        logger.info("Started transcription job %s.", job_name)
    except ClientError:
        logger.exception("Couldn't start transcription job %s.", job_name)
        raise
    else:
        return job


def get_job(job_name, transcribe_client):
    """
    Gets details about a transcription job.

    :param job_name: The name of the job to retrieve.
    :param transcribe_client: The Boto3 Transcribe client.
    :return: The retrieved transcription job.
    """
    try:
        response = transcribe_client.get_transcription_job(
            TranscriptionJobName=job_name)
        job = response['TranscriptionJob']
        logger.info("Got job %s.", job['TranscriptionJobName'])
    except ClientError:
        logger.exception("Couldn't get job %s.", job_name)
        raise
    else:
        return job


def delete_job(job_name, transcribe_client):
    """
    Deletes a transcription job. This also deletes the transcript associated with
    the job.

    :param job_name: The name of the job to delete.
    :param transcribe_client: The Boto3 Transcribe client.
    """
    try:
        transcribe_client.delete_transcription_job(
            TranscriptionJobName=job_name)
        logger.info("Deleted job %s.", job_name)
    except ClientError:
        logger.exception("Couldn't delete job %s.", job_name)
        raise


def retrieve_transcript(filepath, language, speaker, access_key, secret_key, region):
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region
    )
    s3_resource = session.resource('s3')
    transcribe_client = session.client('transcribe')
    bucket_name = f'alex-transcript-{time.time_ns()}'
    bucket = s3_resource.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={
            'LocationConstraint': transcribe_client.meta.region_name})
    media_object_key = f"{uuid.uuid4()}.wav"
    bucket.upload_file(filepath, media_object_key)
    try:
        media_uri = f's3://{bucket.name}/{media_object_key}'
        job_name_simple = f'Alex-Transcript-{time.time_ns()}'
        print(f"Starting transcription job {job_name_simple}.")
        start_job(job_name_simple, media_uri, 'wav', language, speaker, transcribe_client)
        transcribe_waiter = TranscribeCompleteWaiter(transcribe_client)
        transcribe_waiter.wait(job_name_simple)
        job_simple = get_job(job_name_simple, transcribe_client)
        transcript_simple = requests.get(job_simple['Transcript']['TranscriptFileUri']).json()
        print("Deleting demo jobs.")
        delete_job(job_name_simple, transcribe_client)
    finally:
        print("Deleting demo bucket.")
        bucket.objects.delete()
        bucket.delete()
    return transcript_simple


def parse_words(transcript, speaker):
    if speaker == 'interviewee':
        interviewee = 1
    elif speaker == 'interviewer':
        interviewee = 0
    else:
        diarization = {}
        for speaker_segment in transcript['results']['speaker_labels']['segments']:
            for item in speaker_segment['items']:
                diarization[item['start_time']] = {}
                if item['speaker_label'] == 'spk_0':
                    diarization[item['start_time']][item['end_time']] = 0
                else:
                    diarization[item['start_time']][item['end_time']] = 1
    words = []
    for word in transcript['results']['items']:
        if word['type'] == 'pronunciation':
            words.append({
                'service': 'ibm',
                'word': word['alternatives'][0]['content'],
                'start_time': int(float(word['start_time'])*1000),
                'end_time': int(float(word['end_time'])*1000),
                'interviewee': 0
            })
            if speaker in ('interviewee', 'interviewer'):
                words[-1]['interviewee'] = interviewee
            else:
                words[-1]['interviewee'] = diarization[word['start_time']][word['end_time']]
        elif word['type'] == 'punctuation':
            words[-1]['word'] += word['alternatives'][0]['content']
    return words
