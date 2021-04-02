#!/bin/bash
cd /home/ubuntu && \
sudo apt update && \
sudo apt install -y python3-pip unzip && \
wget https://github.com/alexgonca/transcript_interview/archive/refs/heads/main.zip && \
unzip main.zip && \
rm main.zip && \
find ./transcript_interview-main/* -maxdepth 0 -type d,f -exec mv -t ./ {} + && \
rm -R ./transcript_interview-main && \
wget https://raw.githubusercontent.com/internet-scholar/internet_scholar/master/requirements.txt -O requirements2.txt && \
wget https://raw.githubusercontent.com/internet-scholar/internet_scholar/master/internet_scholar.py && \
pip3 install --trusted-host pypi.python.org -r /home/ubuntu/requirements_$8.txt && \
pip3 install --trusted-host pypi.python.org -r /home/ubuntu/requirements2.txt && \
python3 transcriber_cloud.py -b $1 -i $2 -l $3 -s $4 -t $5 -d $6 -p $7 -v $8 && \
sudo shutdown -h now