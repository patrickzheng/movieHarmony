#changes are highlighted in blue
from flask import Markup
from flask import render_template

from flask import jsonify 
#jsonify creates a json representation of the response

from app import app
from cassandra.cluster import Cluster
#importing Cassandra modules from the driver we just installed

# setting up connections to cassandra
cluster = Cluster(['ec2-52-89-21-185.us-west-2.compute.amazonaws.com']) 
session = cluster.connect('playground') 

@app.route('/')
@app.route('/index')
def email():
        stmt = "SELECT asin, imurl FROM metadata limit 20"
        response = session.execute(stmt)
        response_list = []
        for val in response:
             response_list.append(val)
        jsonresponse = [ \
		' \
	<!--div class="span3" style="background-color:red;"--> \
		<img class="img-thumbnail" alt=" " width=100 height=100 src="' + x.imurl + '" style="background-color:black;" > \
	<!--/ div-->' \
		for x in response_list if 'jpg' in x.imurl] 
 	return render_template("base.html", imgUrls = (' '.join(jsonresponse)))
	#return render_template("base.html")
