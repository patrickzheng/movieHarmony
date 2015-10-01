from flask import Blueprint
main = Blueprint('main', __name__)
 
import json
from engine import RecommendationEngine
 
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
 
from flask import Flask, request

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
 
@main.route("/<int:user_id>/ratings/top/<int:count>", methods=["GET"])
def top_ratings(user_id, count):
    logger.debug("User %s TOP ratings requested", user_id)
    top_ratings = recommendation_engine.get_top_ratings(user_id,count)
    return json.dumps(top_ratings)
 
@main.route("/<int:user_id>/ratings/<int:movie_id>", methods=["GET"])
def movie_ratings(user_id, movie_id):
    logger.debug("User %s rating requested for movie %s", user_id, movie_id)
    ratings = recommendation_engine.get_ratings_for_movie_ids(user_id, [movie_id])
    return json.dumps(ratings)
 
 
@main.route("/<int:user_id>/ratings", methods = ["POST"])
def add_ratings(user_id):
    # get the ratings from the Flask POST request object
    ratings_list = request.form.keys()[0].strip().split("\n")
    ratings_list = map(lambda x: x.split(","), ratings_list)
    # create a list with the format required by the negine (user_id, movie_id, rating)
    ratings = map(lambda x: (user_id, int(x[0]), float(x[1])), ratings_list)
    # add them to the model using then engine API
    recommendation_engine.add_ratings(ratings)
 
    return json.dumps(ratings)

@main.route('/')
@main.route('/index')
@main.route('/index.html')
def root():
    return render_template("moviEharmony.html", highmapsdata=[], colorify=[])

#@main.route("/moviesearch")
#def moviesearch():
        #stmt = "SELECT asin, imurl FROM metadata limit 10"
        #response = session.execute(stmt)
        #shuffle(response)
        #output = ""
        #for val in response:
                #if 'gif' not in val.imurl:
                        #output += "".join('<a><img src="' + val.imurl + '" width=200 height=200 /></a>')
        #return output
@main.route("/moviesearch")
def moviesearch():
#@main.route("/moviesearch/(<int:user1>, <int:user2>, <int:power>)")
#def moviesearch(user1, user2, power):
	user1 = 1573033844
	user2 = 1587946258
	power = 50
	count = 10000
    	top_ratings1 = recommendation_engine.get_top_ratings(user1,count)
    	top_ratings2 = recommendation_engine.get_top_ratings(user2,count)
	tr1 = dict({(row.product,row.rating) for row in top_ratings1})
	tr2 = dict({(row.product,row.rating) for row in top_ratings2})
	intersect = set(tr1.keys()).intersection(set(tr2.keys()))
	tr1 = {myKey: tr1[myKey] for myKey in intersect}
	tr2 = {myKey: tr2[myKey] for myKey in intersect}
	finalList = { myKey: (tr1[myKey]/max(tr1.values()),tr2[myKey]/max(tr2.values())*(1+(power-50)/200)) for myKey in intersect}
	sortedfinallist = sorted(finalList.items(), key = lambda item: (min(item[1][0],item[1][1])+numpy.mean(item[1][0],item[1][1])), reverse=True)
	print sortedfinallist
	idlist = [str(x[0]) for x in sortedfinallist[0:8]]
	print idlist
	myString = ",".join(idlist)
	print myString
	stmt = "select * from movie_catalog2 where  pid in (" + myString + ")"
	response = session.execute(stmt)
	print response
	#shuffle(response)
	output = ""
        for val in response:
                if 'gif' not in val.imUrl:
                        output += "".join('<a><img src="' + str(val.imUrl).encode('ascii','ignore') + '" width=200 height=200 alt="' + str(val.title).encode('ascii','ignore') + '" /></a>')
        return Markup(output)

@main.route("/moviesearchrefresh/<user1>/<user2>/<power>")
def moviesearchrefresh(user1, user2, power):
        #user1 = 1573033844
        #user2 = 1587946258
        #power = 50
	print user1
	print user2
	print power
	user1 = int(user1)
	user2 = int(user2)
	power = int(power)
        count = 5000
        top_ratings1 = recommendation_engine.get_top_ratings(user1,count)
        top_ratings2 = recommendation_engine.get_top_ratings(user2,count)
        tr1 = dict({(row.product,row.rating) for row in top_ratings1})
        tr2 = dict({(row.product,row.rating) for row in top_ratings2})
        intersect = set(tr1.keys()).intersection(set(tr2.keys()))
        tr1 = {myKey: tr1[myKey] for myKey in intersect}
        tr2 = {myKey: tr2[myKey] for myKey in intersect}
        finalList = { myKey: (tr1[myKey]/max(tr1.values())*(100-power)/100,tr2[myKey]/max(tr2.values())*power/100) for myKey in intersect}
        sortedfinallist = sorted(finalList.items(), key = lambda item: min(item[1][0],item[1][1]), reverse=True)
        print sortedfinallist
        idlist = [str(x[0]) for x in sortedfinallist[0:8]]
        print idlist
        myString = ",".join(idlist)
        print myString
        stmt = "select * from movie_catalog2 where  pid in (" + myString + ")"
        response = session.execute(stmt)
        print response
        #shuffle(response)
        output = ""
        for val in response:
                if 'gif' not in val.imUrl:
                        output += "".join('<a><img src="' + str(val.imUrl).encode('ascii','ignore') + '" width=200 height=200 alt="' + str(val.title).encode('ascii','ignore') + '" /></a>')
        return Markup(output)
 
 
def create_app(spark_context, dataset_path):
    global recommendation_engine 

    recommendation_engine = RecommendationEngine(spark_context, dataset_path)    
    
    app = Flask(__name__)
    app.register_blueprint(main)
    return app 
