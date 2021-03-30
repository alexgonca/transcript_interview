#!/bin/bash
sudo apt update
sudo apt install -y python3-pip ffmpeg
pip3 install --trusted-host pypi.python.org -r /home/ubuntu/requirements.txt
sudo chmod +x /home/ubuntu/execute_and_shutdown.sh
# screen -S transcribe /home/ubuntu/execute_and_shutdown.sh