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

def GetCountsForHighMaps():
    resp = session.execute(" SELECT * FROM reviewer_profile limit 10 ")
    output = []
    return output

def GetHueCounts():
    resp = session.execute("SELECT region,county,count,maxhue,huevalues,datetaken FROM allhuecountsbatch WHERE granularity='county/all' AND country='United States';")
    output = ""
    for region,county,count,maxhue,huevalues,datetaken in resp:
        try:
            countyname = "%s, %s" % (county, us_state_abbrev[region])
            code = codesbycounty[countyname.lower()]
            output += '$(".highcharts-key-%s").attr("fill","hsl(%d, 100%%, %d%%)");' % (code,
                                                                                      int(maxhue*360),
                                                                                      max(100+50*(-count/100.0),50))

        except:
            # These don't have a code that I know of:
            # Kalawao, HI
            # Brooklyn, NY
            # Staten Island, NY
            pass

    return output

@app.route('/gethuecounts')
def gethuecounts():
    return GetHueCounts()

@app.route('/js/<path:path>')
def send_js(path):
    return send_from_directory('js', path)

@app.route('/css/<path:path>')
def send_css(path):
    return send_from_directory('css', path)

@app.route('/collageplus/<path:path>')
def send_collageplus(path):
    return send_from_directory('collageplus', path)

@app.route('/email.html')
@app.route('/email')
def email():
    # return url_for('static',filename='index.html')
    # return app.route_path
    # return app.root_path
    from cqlengine import connection
    from cassandra.cluster import Cluster
    CASSANDRA_KEYSPACE = "playground"
    # setting up connections to cassandra
    cluster = Cluster(['ec2-52-89-21-185.us-west-2.compute.amazonaws.com'])
    session = cluster.connect('playground')
    # connection.setup(['172.31.39.226', '172.31.39.225'],CASSANDRA_KEYSPACE)
    # cluster = Cluster(['172.31.39.226', '172.31.39.225'])
    # session = cluster.connect(CASSANDRA_KEYSPACE)
    stmt = "SELECT asin, imurl FROM metadata limit 20 ;"
    response = session.execute(stmt)
    response_list = []
    for val in response:
         response_list.append(val)
    jsonresponse = [ \
            	' \
            	<img class="img-thumbnail" alt=" " width=100 height=100 src="' + x.imurl + '" style="background-color:black;" > \
		' \
                for x in response_list if 'jpg' in x.imurl]
    return render_template("base.html", imUrls = Markup(' '.join(jsonresponse)))
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


from datetime import datetime
from elasticsearch import Elasticsearch

# by default we connect to localhost:9200
es = Elasticsearch()

@app.route("/colorsearch/_count")
def colorsearchcount():
    number = es.count(index='inlivingcolor', doc_type='colorcluster')['count']

    return "Number of images processed: %d" % number

@app.route("/moviesearch")
def moviesearch():
	stmt = "SELECT asin, imurl FROM metadata limit 10"
        response = session.execute(stmt)
	shuffle(response)
	output = ""
        for val in response:
		if 'gif' not in val.imurl:
        		output += "".join('<a><img src="' + val.imurl + '" width=200 height=200 /></a>')
	#output += '<script type="text/javascript" style="visibility: hidden"><!-- collage(); //--></script>'
	return output
	#output = '<img class="img-thumbnail" alt=" " width=100 height=100 src="http://ecx.images-amazon.com/images/I/412VACXQBJL._SY300_.jpg" style="background-color:black;" >'
	#return output



@app.route("/colortrends/us/from=<beginningyear>/to=<endingyear>")
def colortrendsus(beginningyear,endingyear):


    from colortrends import getcolortrendsus_png
    pngdata = getcolortrendsus_png(granularity='country/month',
                         state='*',
                         beginningyear=beginningyear,
                         endingyear=endingyear)


    return Response(pngdata, mimetype='image/png')

@app.route("/colortrends/state=<state>/from=<beginningyear>/to=<endingyear>")
def colortrendsstate(state,beginningyear,endingyear):


    from colortrends import getcolortrendsus_png
    pngdata = getcolortrendsus_png(granularity='region/month',
                         state=state,
                         beginningyear=beginningyear,
                         endingyear=endingyear)


    return Response(pngdata, mimetype='image/png')


# @app.route("/colortrends/us/from=<beginningyear>/to=<endingyear>/asimgtag")
# def colortrendsusasimgtag(beginningyear,endingyear):


#     return ""



if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)

