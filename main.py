from retrieve_transcript import Transcript


def main():
    transcript = Transcript()
    try:
        transcript.retrieve_transcript(config={
                                           'label': 'test-separate',
                                           's3_key': 'test-samples/audio_interviewer.mp3',
                                           'speaker': 'interviewer',
                                           'language': "pt-BR"
                                       },
                                       microsoft=True,
                                       google=True,
                                       ibm=True,
                                       aws=True)
        transcript.retrieve_transcript(config={
                                           'label': 'test-separate',
                                           's3_key': 'test-samples/audio_interviewee.mp3',
                                           'speaker': 'interviewee',
                                           'language': "pt-BR"
                                       },
                                       microsoft=True,
                                       google=True,
                                       ibm=True,
                                       aws=True)
        # transcript.export_csv(label='test-sample-separate', interval_in_milliseconds=3000)
    finally:
        transcript.upload_database()


if __name__ == "__main__":
    main()