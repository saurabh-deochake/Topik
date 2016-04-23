'''
Created on Feb 27, 2016

@author: shreepad
'''
from tweepy import StreamListener
import json, time, sys
import MySQLdb
from nltk.stem.snowball import SnowballStemmer
from nltk.corpus import stopwords

class SListener(StreamListener):

    def __init__(self, api = None, fprefix = 'streamer'):
        self.api = api
        self.counter = 0
        self.fprefix = fprefix
        self.output  = open('./data/'+fprefix + '.' 
                            + time.strftime('%Y%m%d-%H%M%S') + '.json', 'w')
        self.delout  = open('delete.txt', 'a')

    def on_data(self, data):

        if  'in_reply_to_status' in data:
            self.on_status(data)
        elif 'delete' in data:
            delete = json.loads(data)['delete']['status']
            if self.on_delete(delete['id'], delete['user_id']) is False:
                return False
        elif 'limit' in data:
            if self.on_limit(json.loads(data)['limit']['track']) is False:
                return False
        elif 'warning' in data:
            warning = json.loads(data)['warnings']
            print (warning['message'])
            return False

    def on_status(self, status):
        
        json_status = json.loads(status)
        
        
        #Need to put this a separate funtion
        #---------Extract fields from post------
        data = {}
        
        if("retweeted_status" not in json_status):
			data["p_id"] = json_status["id_str"]
			data["timestamp"] = json_status["timestamp_ms"]
			data["text"] = json_status["text"]
			data["u_id"] = json_status["user"]["id_str"]
			data["r_p_id"] = json_status["in_reply_to_status_id_str"]
			data["r_u_id"] = json_status["in_reply_to_user_id_str"]
                
			lat = 0
			lng =0
			for l in (json_status["place"]["bounding_box"]["coordinates"][0]):
				lng += l[0]
				lat += l[1]

			lng /= len(json_status["place"]["bounding_box"]["coordinates"][0])
			lat /= len(json_status["place"]["bounding_box"]["coordinates"][0])
        
			data["lng"] = lng
			data["lat"] = lat
        else:
			return
        #---------------------------------------
        
        #--Stop and stem words------------------
        stop = stopwords.words('english')
        stemmer = SnowballStemmer("english")
        
        #Remove Stop words
        words = [i for i in data["text"].split() if i not in stop]
        
        #Stemming
        stemwords = []
        for w in words:
			stemwords.append(stemmer.stem(w))
        
        data["text"] = " ".join(stemwords)
        
        #---------------------------------------
        
        #--Put data to database-----------------
        db = MySQLdb.connect(host="localhost",    # your host, usually localhost
                             user="spd",         # your username
                             passwd="meraPassword",  # your password
                             db="topik")        # name of the data base

        #----Post-------------------------------
        curP = db.cursor()
        curP.execute("INSERT INTO posts (ts,uid,words,lat,lng,pid,rpid,ruid) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", (data["timestamp"], data["u_id"], data["text"], data["lat"], data["lng"],data["p_id"],data["r_p_id"],data["r_u_id"]))
        print "Auto Increment ID: %s" % curP.lastrowid
        
        #----Graph------------------------------
        curGE = db.cursor()
        curGVu = db.cursor()
        curGVv = db.cursor()
        
        
        if(data["r_u_id"]):
			curGE.execute("INSERT IGNORE INTO graph_edges SET uid = %s, vid = %s;", (data["u_id"],data["r_u_id"]))
			#print "Auto Increment ID: %s" % curGE.lastrowid
			curGVv.execute("INSERT IGNORE INTO graph_vertices SET uid = %s;",[data["r_u_id"]]);
			print data["r_u_id"]
        
        curGVu.execute("INSERT IGNORE INTO graph_vertices SET uid=%s;",[data["u_id"]]);
        print data["u_id"]
        #---------------------------------------
        
        db.commit()
        curP.close()
        curGE.close()
        curGVu.close()
        curGVv.close()
        db.close()
        #---------------------------------------
        
        
        
        
        self.output.write(json.dumps(data) + "\n")

        self.counter += 1

        if self.counter >= 20000:
            self.output.close()
            path = './data/'
            self.output = open( path + self.fprefix + '.' 
                               + time.strftime('%Y%m%d-%H%M%S') + '.json', 'w')
            self.counter = 0

        return

    def on_delete(self, status_id, user_id):
        self.delout.write( str(status_id) + "\n")
        return

    def on_limit(self, track):
        sys.stderr.write(str(track) + "\n")
        return

    def on_error(self, status_code):
        sys.stderr.write('Error: ' + str(status_code) + "\n")
        return False

    def on_timeout(self):
        sys.stderr.write("Timeout, sleeping for 60 seconds...\n")
        time.sleep(60)
        return 
