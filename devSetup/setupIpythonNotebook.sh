sudo apt-get install python-pip
sudo pip install --upgrade pip 
sudo apt-get install ipython-notebook
sudo chmod +w /usr/local/lib/python2.7/dist-packages
sudo pip install certifi
sudo pip install jsonschema

IPYTHON_OPTS="notebook" pyspark --master spark://ip-172-31-45-91:7077 --executor-memory 6400M --driver-memory 6400M
