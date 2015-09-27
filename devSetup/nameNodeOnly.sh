#!/bin/bash

. ~/.profile
. ~/globalVars.sh

sudo apt-get update
sudo apt-get install ssh rsync
ssh-keygen -f ~/.ssh/id_rsa -t rsa -P ""
sudo cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
ssh localhost
