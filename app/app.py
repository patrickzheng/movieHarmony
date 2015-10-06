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

@main.route("/queryuser/<user1>")
def queryuser(user1):
	print user1
	user1 = int(user1)
	stmt = "SELECT * FROM userprofile9 where uid = " + str(user1) + " ;"
	response = session.execute(stmt)
	finalList =  response[0].ratings
        sortedfinallist = sorted(finalList.items(), key = lambda item: item[1], reverse=True)
        print sortedfinallist
        idlist = [str("'" + x[0] + "'") for x in sortedfinallist[0:8]]
        print idlist
        myString = ",".join(idlist)
        print myString
        stmt = "select * from metadata where asin in (" + myString + ")"
        response = session.execute(stmt)
        print response
        #shuffle(response)
        #output = str(user1)
        output = ""
        for val in response:
                if 'gif' not in val.imurl:
                        output += "".join('<a><img src="' + str(val.imurl).encode('ascii','ignore') + '" width=175 height=175 title="' + str(val.title).encode('ascii','ignore') + '" alt="' + str(val.title).encode('ascii','ignore') + '" /></a>')
        return Markup(output)

@main.route("/submitmoviequery/<movieid>")
def submitmoviequery(movieid):
	print movieid
	stmt = "SELECT * FROM metadata where asin = '" + str(movieid) + "' ;"
        response = session.execute(stmt)
        print response
        output = ""
        for val in response:
                if 'gif' not in val.imurl:
                        output += "".join('<a><img src="' + str(val.imurl).encode('ascii','ignore') + '" width=150 height=150 title="' + str(val.title).encode('ascii','ignore') + '" alt="' + str(val.title).encode('ascii','ignore') + '" /></a>')
        return Markup(output)

@main.route("/submitreview/<reviewerID>/<asin>/<overall>/<summary>/<reviewText>")
def submitreview(reviewerID, asin, overall, summary, reviewText):
	print reviewerID
	print asin
	print overall
	print summary
	print reviewText
	jsonMessage = json.dumps({"reviewerID": reviewerID, "asin": asin, "overall": float(overall), "summary": summary, "reviewText": reviewText, "helpful": None, "reviewerName": None, "reviewTime": None, "unixReviewTime": None});
	from kafka import SimpleProducer, KafkaClient
	# To send messages synchronously
	kafka = KafkaClient('ec2-52-26-15-148.us-west-2.compute.amazonaws.com:9092')
	producer = SimpleProducer(kafka)
	producer.send_messages(b'moviereview9', bytes(jsonMessage))
	return "Review successfully submitted."


@main.route("/query2user/<user1>/<user2>")
def query2user(user1,user2):
	print user1
	user1 = int(user1)
	stmt = "SELECT * FROM userprofile9 where uid = " + str(user1) + " ;"
	response = session.execute(stmt)
	finalList =  response[0].ratings
        sortedfinallist = sorted(finalList.items(), key = lambda item: item[1], reverse=True)
        print sortedfinallist
        idlist = [str("'" + x[0] + "'") for x in sortedfinallist[0:8]]
        print idlist
        myString = ",".join(idlist)
        print myString
        stmt = "select * from metadata where asin in (" + myString + ")"
        response = session.execute(stmt)
        print response
        #output1 = str(user1)
        output1 = ""
        for val in response:
                if 'gif' not in val.imurl:
                        output1 += "".join('<a><img src="' + str(val.imurl).encode('ascii','ignore') + '" width=200 height=200 title="' + str(val.title).encode('ascii','ignore') + '" alt="' + str(val.title).encode('ascii','ignore') + '" /></a>')
	print user2
	user2 = int(user2)
	stmt = "SELECT * FROM userprofile9 where uid = " + str(user2) + " ;"
	response = session.execute(stmt)
	finalList =  response[0].ratings
        sortedfinallist = sorted(finalList.items(), key = lambda item: item[1], reverse=True)
        print sortedfinallist
        idlist = [str("'" + x[0] + "'") for x in sortedfinallist[0:8]]
        print idlist
        myString = ",".join(idlist)
        print myString
        stmt = "select * from metadata where asin in (" + myString + ")"
        response = session.execute(stmt)
        print response
        #output2 = str(user2)
        output2 = ""
        for val in response:
                if 'gif' not in val.imurl:
                        output2 += "".join('<a><img src="' + str(val.imurl).encode('ascii','ignore') + '" width=200 height=200 title="' + str(val.title).encode('ascii','ignore') + '" alt="' + str(val.title).encode('ascii','ignore') + '" /></a>')
        return Markup(output1 + '<br>' + output2)
 
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
	def checkAndUpdate(userid,count):
		stmt = "select * from recommendations9 where uid = " + str(userid) + ";"
		response = list(session.execute(stmt))
		if len(response) > 0: 
			return response
		else:
			def syncToCassandra(d_iter):
        			from cqlengine import columns
        			from cqlengine.models import Model
        			from cqlengine import connection
        			from cqlengine.management import sync_table
        			CASSANDRA_KEYSPACE = "playground"
        			connection.setup(['172.31.39.226'], CASSANDRA_KEYSPACE)
        			class recommendations9(Model):
               				uid = columns.Integer(primary_key=True)
               				mid = columns.Integer(primary_key=True)
               				rating = columns.Float()
        			sync_table(recommendations9)
        			for d in d_iter:
               				recommendations9.create(**d)
			#syncToCassandra([])
			recommendations = recommendation_engine.get_top_ratings(userid,count)
			rRdd0 = recommendation_engine.sc.parallelize(recommendations)
			rRdd1 = rRdd0.map(lambda x: {"uid":x[0], "mid":x[1], "rating":x[2]})
			rRdd1.foreachPartition(syncToCassandra)
                stmt = "select * from recommendations9 where uid = " + str(userid) + ";"
                response = session.execute(stmt)
		return response

        #top_ratings1 = recommendation_engine.get_top_ratings(user1,count)
        #top_ratings2 = recommendation_engine.get_top_ratings(user2,count)
	top_ratings1 = checkAndUpdate(user1,count)
	top_ratings2 = checkAndUpdate(user2,count)
        tr1 = dict({(row.mid,row.rating) for row in top_ratings1})
        tr2 = dict({(row.mid,row.rating) for row in top_ratings2})
        #intersect = set(tr1.keys()).intersection(set(tr2.keys()))
        intersect = set(tr1.keys()).union(set(tr2.keys()))
        tr1 = {myKey: tr1.get(myKey,0) for myKey in intersect}
        tr2 = {myKey: tr2.get(myKey,0) for myKey in intersect}
	tr1Max = max(tr1.values())
	tr2Max = max(tr2.values())
	tr1Min = min(tr1.values())
	tr2Min = min(tr2.values())
	tr1Range = tr1Max - tr1Min
	tr2Range = tr2Max - tr2Min
        #finalList = { myKey: (tr1[myKey] / tr1Max * (100 - power) / 100, tr2[myKey] / tr2Max * power / 100) for myKey in intersect}
        finalList = { myKey: ((tr1[myKey] - tr1Min)/ tr1Range * (100 - power) / 100, (tr2[myKey] - tr2Min) / tr2Range * power / 100) for myKey in intersect}
        #sortedfinallist = sorted(finalList.items(), key = lambda item: min(item[1][0],item[1][1])+(item[1][0]+item[1][1])/2, reverse=True)
        #sortedfinallist = sorted(finalList.items(), key = lambda item: (item[1][0] + item[1][1]) / 2, reverse=True)
        sortedfinallist = sorted(finalList.items(), key = lambda item: 0.9 * (item[1][0] + item[1][1]) / 2 + 0.1 * (1 - abs(item[1][0] - item[1][1]) ** 2) / 2, reverse=True)
        print sortedfinallist
        idlist = [str(x[0]) for x in sortedfinallist[0:8]]
        print idlist
        myString = ",".join(idlist)
        print myString
        #stmt = "select * from movie_catalog2 where  pid in (" + myString + ")"
        stmt = "select * from movieprofile9 where mid in (" + myString + ")"
        response = session.execute(stmt)
        print response
        #shuffle(response)
        output = ""
        #output = str(user1) + " " + str(user2) 
        for val in response:
                if 'gif' not in val.imurl:
                        output += "".join('<a><img src="' + str(val.imurl).encode('ascii','ignore') + '" width=200 height=200 title="' + str(val.title).encode('ascii','ignore') + '" alt="' + str(val.title).encode('ascii','ignore') + '" /></a>')
        return Markup(output)
 
def create_app(spark_context, dataset_path):
    global recommendation_engine 

    recommendation_engine = RecommendationEngine(spark_context, dataset_path)    
    
    app = Flask(__name__)
    app.register_blueprint(main)
    return app 
