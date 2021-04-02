def split_words_protagonism(words):
    protagonist_words = []
    non_protagonist_words = []
    for word in words:
        if word['protagonist'] == 1:
            protagonist_words.append({
                'seq_num': word['seq_num'],
                'word': word['word'],
                'start_time': word['start_time'],
                'end_time': word['end_time']
            })
        else:
            non_protagonist_words.append({
                'seq_num': word['seq_num'],
                'word': word['word'],
                'start_time': word['start_time'],
                'end_time': word['end_time']
            })
    return protagonist_words, non_protagonist_words


def parse_words_microsoft(transcript, speaker_type):
    words = []
    if (speaker_type == "interviewee") or (speaker_type == "single"):
        protagonist = 1
    else:
        protagonist = 0
    seq_num = 1
    for phrase in transcript['recognizedPhrases']:
        if speaker_type == "both":
            if phrase['speaker'] == 1:
                protagonist = 0
            else:
                protagonist = 1
        phrase_with_punctuation = phrase['nBest'][0].get('display', '').split()
        duration_word = int( ( phrase['durationInTicks'] / 10000 ) // len(phrase_with_punctuation))
        offset_word = int( phrase['offsetInTicks'] / 10000 )
        end_phrase = offset_word + int( phrase['durationInTicks'] / 10000 )
        for word in phrase_with_punctuation:
            words.append({
                'seq_num': seq_num,
                'word': word,
                'start_time': offset_word,
                'end_time': offset_word + duration_word - 1,
                'protagonist': protagonist
            })
            seq_num = seq_num + 1
            offset_word = offset_word + duration_word
        if len(words) > 0:
            words[-1]['end_time'] = end_phrase
    return words


def parse_words_google(transcript, speaker_type):
    words = []
    seq_num = 1
    if speaker_type == "both":
        for word in transcript['results'][-1]['alternatives'][0]['words']:
            if word['speakerTag'] == 1:
                protagonist = 0
            else:
                protagonist = 1
            words.append({
                'seq_num': seq_num,
                'word': word['word'],
                'start_time': int(float(word['startTime'][:-1]) * 1000),
                'end_time': int(float(word['endTime'][:-1]) * 1000),
                'protagonist': protagonist
            })
            seq_num = seq_num + 1
    elif speaker_type in ['interviewee', 'interviewer', 'single']:
        if (speaker_type == 'interviewee') or (speaker_type == 'single'):
            protagonist = 1
        else:
            protagonist = 0
        for word_cluster in transcript['results']:
            for word in word_cluster['alternatives'][0]['words']:
                words.append({
                    'seq_num': seq_num,
                    'word': word['word'],
                    'start_time': int(float(word['startTime'][:-1]) * 1000),
                    'end_time': int(float(word['endTime'][:-1]) * 1000),
                    'protagonist': protagonist
                })
                seq_num = seq_num + 1
    else:
        raise TypeError('Unknown speaker type: {speaker_type}'.format(speaker_type=speaker_type))
    return words


def parse_words_ibm(transcript, speaker_type):
    words = []
    if speaker_type in ("interviewee", "both"):
        protagonist = 1
    else:
        protagonist = 0
    seq_num = 1
    for outer_result in transcript['results']:
        for inner_result in outer_result['results']:
            for word in inner_result['alternatives'][0]['timestamps']:
                words.append({
                    'seq_num': seq_num,
                    'word': word[0],
                    'start_time': int(word[1] * 1000),
                    'end_time': int(word[2] * 1000),
                    'protagonist': protagonist
                })
                seq_num = seq_num + 1
    return words


def parse_words_aws(transcript, speaker_type):
    if (speaker_type == 'interviewee') or (speaker_type == 'single'):
        protagonist = 1
    elif speaker_type == 'interviewer':
        protagonist = 0
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
    seq_num = 1
    for word in transcript['results']['items']:
        if word['type'] == 'pronunciation':
            words.append({
                'seq_num': seq_num,
                'word': word['alternatives'][0]['content'],
                'start_time': int(float(word['start_time'])*1000),
                'end_time': int(float(word['end_time'])*1000),
                'protagonist': 0
            })
            seq_num = seq_num + 1
            if speaker_type in ('interviewee', 'interviewer', 'single'):
                words[-1]['protagonist'] = protagonist
            else:
                words[-1]['protagonist'] = diarization[word['start_time']][word['end_time']]
        elif word['type'] == 'punctuation':
            words[-1]['word'] += word['alternatives'][0]['content']
    return words


def parse_words(transcript, speaker_type, service):
    if service == "microsoft":
        words = parse_words_microsoft(transcript=transcript, speaker_type=speaker_type)
    elif service == "google":
        words = parse_words_google(transcript=transcript, speaker_type=speaker_type)
    elif service == "aws":
        words = parse_words_aws(transcript=transcript, speaker_type=speaker_type)
    elif service == "ibm":
        words = parse_words_ibm(transcript=transcript, speaker_type=speaker_type)
    else:
        raise TypeError(f"Invalid service: {service}")
    return split_words_protagonism(words)