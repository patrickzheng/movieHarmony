import time, sys, cherrypy, os
from paste.translogger import TransLogger
from app import create_app
from pyspark import SparkContext, SparkConf

#changes are highlighted in blue
from random import shuffle
from flask import Markup
from flask import render_template

from flask import jsonify
#jsonify creates a json representation of the response

# from app import app
from cassandra.cluster import Cluster
#importing Cassandra modules from the driver we just installed
from cqlengine import connection
from cassandra.cluster import Cluster
CASSANDRA_KEYSPACE = "playground"
# setting up connections to cassandra
cluster = Cluster(['ec2-52-88-117-89.us-west-2.compute.amazonaws.com'])
session = cluster.connect('playground')


from flask import Flask, request, send_from_directory, render_template, url_for
app = Flask(__name__)
from flask import Response
from os.path import abspath, dirname

@app.route('/')
@app.route('/index')
@app.route('/index.html')
def root():
    # return url_for('static',filename='index.html')
    # return app.route_path
    # return app.root_path
    from cqlengine import connection
    from cassandra.cluster import Cluster
    CASSANDRA_KEYSPACE = "playground"
    # setting up connections to cassandra
    cluster = Cluster(['ec2-52-88-117-89.us-west-2.compute.amazonaws.com'])
    session = cluster.connect('playground')
    return render_template("moviEharmony.html", highmapsdata=[], colorify=[])


def init_spark_context():
    # load spark context
    conf = SparkConf().setAppName("movie_recommendation-server")
    # IMPORTANT: pass aditional Python modules to each worker
    sc = SparkContext(conf=conf, pyFiles=['engine.py', 'app.py'])
 
    return sc
 
 
def run_server(app):
 
    # Enable WSGI access logging via Paste
    app_logged = TransLogger(app)
 
    # Mount the WSGI callable object (app) on the root directory
    cherrypy.tree.graft(app_logged, '/')
 
    # Set the configuration of the web server
    cherrypy.config.update({
        'engine.autoreload.on': True,
        'log.screen': True,
        'server.socket_port': 5432,
        'server.socket_host': '0.0.0.0'
    })
 
    # Start the CherryPy WSGI web server
    cherrypy.engine.start()
    cherrypy.engine.block()
 
 
if __name__ == "__main__":
    # Init spark context and load libraries
    sc = init_spark_context()
    dataset_path = os.path.join('datasets', 'ml-latest')
    app = create_app(sc, dataset_path)
 
    # start web server
    run_server(app)

