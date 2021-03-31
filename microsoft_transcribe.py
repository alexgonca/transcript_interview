import swagger_client as cris_client
import logging
import time
import requests
from azure.storage.blob import BlobServiceClient
import uuid
import json
from datetime import datetime, timedelta
from azure.storage.blob import BlobClient, generate_blob_sas, BlobSasPermissions


# The client was generated via swagger following this instructions:
# https://docs.microsoft.com/en-us/azure/cognitive-services/speech-service/swagger-documentation

def _paginate(api, paginated_object):
    """
    The autogenerated client does not support pagination. This function returns a generator over
    all items of the array that the paginated object `paginated_object` is part of.
    """
    yield from paginated_object.values
    typename = type(paginated_object).__name__
    auth_settings = ["apiKeyHeader", "apiKeyQuery"]
    while paginated_object.next_link:
        link = paginated_object.next_link[len(api.api_client.configuration.host):]
        paginated_object, status, headers = api.api_client.call_api(link, "GET",
            response_type=typename, auth_settings=auth_settings)

        if status == 200:
            yield from paginated_object.values
        else:
            raise Exception(f"could not receive paginated data: status {status}")


def delete_all_transcriptions(api):
    """
    Delete all transcriptions associated with your speech resource.
    """
    logging.info("Deleting all existing completed transcriptions.")

    # get all transcriptions for the subscription
    transcriptions = list(_paginate(api, api.get_transcriptions()))

    # Delete all pre-existing completed transcriptions.
    # If transcriptions are still running or not started, they will not be deleted.
    for transcription in transcriptions:
        transcription_id = transcription._self.split('/')[-1]
        logging.debug(f"Deleting transcription with id {transcription_id}")
        try:
            api.delete_transcription(transcription_id)
        except cris_client.rest.ApiException as exc:
            logging.error(f"Could not delete transcription {transcription_id}: {exc}")


def retrieve_transcript(filepath, language, speaker,
                        subscription_key, account_name, account_key, connection_string, service_region):
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_name = str(uuid.uuid4())
    container_client = blob_service_client.get_container_client(container_name)
    container_client.create_container()
    try:
        blob_client = container_client.get_blob_client('audio.wav')
        with open(filepath, "rb") as data:
            blob_client.upload_blob(data, blob_type="BlockBlob")
        sas_blob = generate_blob_sas(account_name=account_name,
                                     container_name=container_name,
                                     blob_name='audio.wav',
                                     account_key=account_key,
                                     permission=BlobSasPermissions(read=True),
                                     expiry=datetime.utcnow() + timedelta(hours=24))
        uri = blob_client.url + '?' + sas_blob

        logging.info("Starting transcription client...")

        # configure API key authorization: subscription_key
        configuration = cris_client.Configuration()
        configuration.api_key["Ocp-Apim-Subscription-Key"] = subscription_key
        configuration.host = f"https://{service_region}.api.cognitive.microsoft.com/speechtotext/v3.0"

        # create the client object and authenticate
        client = cris_client.ApiClient(configuration)

        # create an instance of the transcription api class
        api = cris_client.DefaultApi(api_client=client)

        # Specify transcription properties by passing a dict to the properties parameter. See
        # https://docs.microsoft.com/azure/cognitive-services/speech-service/batch-transcription#configuration-properties
        # for supported parameters.
        properties = {
            "punctuationMode": "Automatic",
            "profanityFilterMode": "None",
            "wordLevelTimestampsEnabled": True,
            "diarizationEnabled": (speaker == "both"),
            "timeToLive": "PT1H"
        }

        # Use base models for transcription.
        transcription_definition = cris_client.Transcription(
            display_name="Simple transcription",
            description="Simple transcription description",
            locale=language,
            content_urls=[uri],
            properties=properties
        )

        created_transcription, status, headers = api.create_transcription_with_http_info(transcription=transcription_definition)

        # get the transcription Id from the location URI
        transcription_id = headers["location"].split("/")[-1]

        # Log information about the created transcription. If you should ask for support, please
        # include this information.
        logging.info(f"Created new transcription with id '{transcription_id}' in region {service_region}")

        logging.info("Checking status.")

        transcript = {}
        completed = False
        while not completed:
            # wait for 5 seconds before refreshing the transcription status
            time.sleep(5)

            transcription = api.get_transcription(transcription_id)
            logging.info(f"Transcriptions status: {transcription.status}")

            if transcription.status in ("Failed", "Succeeded"):
                completed = True

            if transcription.status == "Succeeded":
                pag_files = api.get_transcription_files(transcription_id)
                for file_data in _paginate(api, pag_files):
                    if file_data.kind != "Transcription":
                        continue

                    results_url = file_data.links.content_url
                    results = requests.get(results_url)
                    transcript = json.loads(results.content)
            elif transcription.status == "Failed":
                print(f"Transcription failed: {transcription.properties.error.message}")
                raise Exception(f"Transcription failed: {transcription.properties.error.message}")
    finally:
        delete_all_transcriptions(api)
        container_client.delete_container()
    return transcript


def parse_words(transcript, speaker):
    words = []
    if speaker == "interviewee":
        interviewee = 1
    else:
        interviewee = 0
    for phrase in transcript['recognizedPhrases']:
        if speaker == "both":
            if phrase['speaker'] == 1:
                interviewee = 0
            else:
                interviewee = 1
        phrase_with_punctuation = phrase['nBest'][0].get('display', '').split()
        duration_word = int( ( phrase['durationInTicks'] / 10000 ) // len(phrase_with_punctuation))
        offset_word = int( phrase['offsetInTicks'] / 10000 )
        end_phrase = offset_word + int( phrase['durationInTicks'] / 10000 )
        for word in phrase_with_punctuation:
            words.append({
                'service': 'microsoft',
                'word': word,
                'start_time': offset_word,
                'end_time': offset_word + duration_word - 1,
                'interviewee': interviewee
            })
            offset_word = offset_word + duration_word
        if len(words) > 0:
            words[-1]['end_time'] = end_phrase
    return words