#!/bin/bash

. ~/.profile
. ~/globalVars.sh

for i in "$NamenodeDSN" "$Datanode1DSN" "$Datanode2DSN" "$Datanode3DSN" "$Datanode4DSN"
do
scp -o "StrictHostKeyChecking no" -i ~/.ssh/personal-aws.pem ~/.ssh/personal-aws.pem ubuntu@$i:~/.ssh
scp -o "StrictHostKeyChecking no" -i ~/.ssh/personal-aws.pem ~/insight/*.sh ubuntu@$i:~/
done
