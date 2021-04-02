from internet_scholar import AthenaLogger, read_dict_from_s3, save_data_in_s3
from transcriber_parser import parse_words
import argparse
from collections import OrderedDict
import logging
import datetime


# todo create routine to export data as csv

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-b', '--bucket', help='S3 Bucket with data', required=True)
    parser.add_argument('-i', '--identifier', help='File identifier on the cloud', required=True)
    parser.add_argument('-l', '--language', help='Audio language', required=True)
    parser.add_argument('-s', '--speaker', help="Speaker's name", required=True)
    parser.add_argument('-t', '--speaker_type', help="Speaker's type (interviewee, interviewer, single, both)",
                        required=True)
    parser.add_argument('-d', '--performance_date', help="Performance date", required=True)
    parser.add_argument('-p', '--project', help="Project", required=True)
    parser.add_argument('-v', '--service', help="Service (aws, microsoft, google, ibm)", required=True)
    args = parser.parse_args()

    config = read_dict_from_s3(bucket=args.bucket, key='config/config.json')

    logger = AthenaLogger(
        app_name=f"transcribe_{args.service}_{args.project}_{args.speaker}_{args.speaker_type}_{args.performance_date}",
        s3_bucket=args.bucket,
        athena_db=config['aws']['athena'])

    try:
        if args.service == "microsoft":
            from transcribe_microsoft import retrieve_transcript, delete_uploaded_file
        elif args.service == "google":
            from transcribe_google import retrieve_transcript, delete_uploaded_file
        elif args.service == "aws":
            from transcribe_aws import retrieve_transcript, delete_uploaded_file
        elif args.service == "ibm":
            from transcribe_ibm import retrieve_transcript, delete_uploaded_file
        else:
            raise Exception(f"Invalid service: {args.service}")

        logging.info(f'Retrieve transcript on {args.service}')
        metadata = {
            'started_at': str(datetime.datetime.utcnow()),
            'language': args.language,
            'audio_storage': args.identifier
        }
        transcript = retrieve_transcript(identifier=args.identifier,
                                         language=args.language,
                                         speaker_type=args.speaker_type,
                                         service_config=config[args.service])
        metadata['finished_at'] = str(datetime.datetime.utcnow())
        transcript['metadata_internet_scholar'] = metadata

        logging.info(f'Succesfully retrieved transcript on {args.service}')
        partitions = OrderedDict()
        partitions['service'] = args.service
        partitions['project'] = args.project
        partitions['speaker'] = args.speaker
        partitions['performance_date'] = args.performance_date
        partitions['speaker_type'] = args.speaker_type
        logging.info(f'Save transcript on S3')
        save_data_in_s3(content=transcript,
                        s3_bucket=args.bucket,
                        s3_key='transcript.json',
                        prefix='transcript',
                        partitions=partitions)

        logging.info(f'Parse words')
        protagonist_words, non_protagonist_words = parse_words(transcript=transcript,
                                                               speaker_type=args.speaker_type,
                                                               service=args.service)
        partitions = OrderedDict()
        partitions['project'] = args.project
        partitions['speaker'] = args.speaker
        partitions['performance_date'] = args.performance_date
        partitions['service'] = args.service
        logging.info('Save words on S3')
        if len(protagonist_words) > 0:
            partitions['protagonist'] = 1
            save_data_in_s3(content=protagonist_words,
                            s3_bucket=args.bucket,
                            s3_key='word.json',
                            prefix='word',
                            partitions=partitions)
        if len(non_protagonist_words) > 0:
            partitions['protagonist'] = 0
            save_data_in_s3(content=non_protagonist_words,
                            s3_bucket=args.bucket,
                            s3_key='word.json',
                            prefix='word',
                            partitions=partitions)

    finally:
        delete_uploaded_file(args.identifier, config[args.service])
        logger.save_to_s3()


if __name__ == '__main__':
    main()