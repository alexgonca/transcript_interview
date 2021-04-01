from internet_scholar import AthenaLogger, read_dict_from_s3, save_data_in_s3
from parse_words import parse_words
import argparse
from collections import OrderedDict
import logging


# todo create routine to export data as csv
# todo test with audio samples

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
        app_name=f"transcribe_{args.service}_{args.project}_{args.speaker}_{args.speaker_type}_{args.performed_date}",
        s3_bucket=args.bucket,
        athena_db=config['aws']['athena'])

    try:
        if args.service == "microsoft":
            from microsoft_transcribe import retrieve_transcript
        elif args.service == "google":
            from google_transcribe import retrieve_transcript
        elif args.service == "aws":
            from aws_transcribe import retrieve_transcript
        elif args.service == "ibm":
            from ibm_transcribe import retrieve_transcript
        else:
            raise Exception(f"Invalid service: {args.service}")
        logging.info(f'Retrieve transcript on {args.service}')
        transcript = retrieve_transcript(identifier=args.uri,
                                         language=args.language,
                                         speaker_type=args.speaker_type,
                                         service_config=config[args.service])
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
        words = parse_words(transcript=transcript, speaker_type=args.speaker_type, service=args.service)
        partitions = OrderedDict()
        partitions['project'] = args.project
        partitions['speaker'] = args.speaker
        partitions['performance_date'] = args.performance_date
        partitions['service'] = args.service
        partitions['speaker_type'] = args.speaker_type
        logging.info('Save words on S3')
        save_data_in_s3(content=words,
                        s3_bucket=args.bucket,
                        s3_key='wrod.json',
                        prefix='word',
                        partitions=partitions)
    finally:
        logger.save_to_s3()


if __name__ == '__main__':
    main()