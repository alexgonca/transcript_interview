#!/bin/bash
sudo apt update
sudo apt install -y python3-pip ffmpeg
pip3 install --trusted-host pypi.python.org -r /home/ubuntu/requirements.txt
python3 main.py
sudo shutdown -h now