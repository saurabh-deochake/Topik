from flask import Flask,request
import json
import query_processing
import MySQLdb
import copy

app = Flask(__name__)

@app.route('/users',methods=['GET'])
def show_user_profile():
    # show the user profile for that user
    if request.method == 'GET':
		
		query = request.args.get('query', '')
		lat = float(request.args.get('lat', ''))
		lng = float(request.args.get('lng', ''))
		rad = int(request.args.get('rad', ''))
		k = int(request.args.get('k', ''))
		
		#q = query_processing.Query("food",40.497867,-74.446673,5)
		q = query_processing.Query(query,lat,lng,rad)
		q.processQuery()
		#k=5
		alpha = 0.2
		json_output = q.findTopK(k,alpha)
		
		db = MySQLdb.connect(host="localhost",    # your host, usually localhost
                             user="spd",         # your username
                             passwd="meraPassword",  # your password
                             db="topik")        # name of the data base
		
		
		curUsers = db.cursor()
		###curText = db.cursor()
		LUsers = []
		
		jsonObject = {
			"userID" : "",
			"name" : "",
			"handle": "",
			"image" : ""
		}
		
		for user in json_output:
			
			curUsers.execute("SELECT * FROM graph_vertices where uid = %s;",[user["user"]])
			
			row = curUsers.fetchall()
			print json.dumps(LUsers)
			
			jsonObject["userID"] = row[0][0]
			jsonObject["name"] = row[0][1]
			jsonObject["handle"] = row[0][2]
			jsonObject["image"] = row[0][3]
			
			JSON = copy.deepcopy(jsonObject)
			
			LUsers.append(JSON)
			
		return json.dumps(LUsers)
	

if __name__ == "__main__":
    # here is starting of the development HTTP server
    app.run(debug=True)
 
