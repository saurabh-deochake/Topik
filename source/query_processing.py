import MySQLdb
from nltk.stem.snowball import SnowballStemmer
from nltk.corpus import stopwords
from geopy.distance import vincenty


class Query():
	def __init__(self, text, lat, lng,r):
		self.text = text
		self.w = 5.0
		self.popDepth = 5
		self.lat = lat
		self.lng = lng
		self.r = r

	def processQuery(self):
		stop = stopwords.words('english')
		stemmer = SnowballStemmer("english")
        
		words = [i for i in self.text.split() if i not in stop]
		
		stemwords = []
		for w in words:
			stemwords.append(stemmer.stem(w))
        
		self.processedText = " ".join(stemwords)

	def showProcessedText(self):
		print self.processedText

	def findTopTextResults(self):
		
		db = MySQLdb.connect(host="localhost",    # your host, usually localhost
                             user="spd",         # your username
                             passwd="meraPassword",  # your password
                             db="topik")        # name of the data base
		
		curP = db.cursor()
		curT = db.cursor()
		curPop = db.cursor()
		
		curP.execute("SELECT * from posts;")
		curT.execute("CREATE OR REPLACE TABLE temp_textScore(score DOUBLE,uid NVARCHAR(20),pid NVARCHAR(20) PRIMARY KEY) engine=InnoDB;")
		db.commit()
		
		rows = curP.fetchall()
		
		for row in rows:
			match = 0
			scoreMultiplier = self.w
			for word in row[2].split():
				if word in self.processedText.split():
					match +=1
			
			if match > 0:
				tweet_ids = []
				tweet_ids.append([])
				tweet_ids[0].append(row[5])
				i=0
				
			#	print tweet_ids
				while i <= self.popDepth:
			#		print "i = ",i
					tweet_ids.append([])
					for j in tweet_ids[i]:
			#			print '\t',j
						curPop.execute("SELECT pid FROM posts WHERE rpid = %s;",[j])
						tempList = []
						tempList.extend(list(curPop.fetchall()))
			#			print '\t List',tempList
			#			print '\t Len:',len(tempList)
						if len(tempList) > 0:
			#				print '\t\tTweet IDs:',tweet_ids
							tweet_ids[i+1].append(tempList)
					i += 1
					scoreMultiplier += (float(1)/(i+1) * len(tweet_ids[i]))
					#print "scoreMultiplier : ",(float(1)/(i+1) * len(tweet_ids[i]))
			#	print match*scoreMultiplier
			
			curT.execute("INSERT INTO temp_textScore (score,uid,pid) VALUES (%s,%s,%s)",(match*scoreMultiplier,row[1],row[5]))
			
		
		
		db.commit()
		curP.close()
		curT.close()
		db.close()


	def findTopDistResults(self):
		db = MySQLdb.connect(host="localhost",    # your host, usually localhost
                             user="spd",         # your username
                             passwd="meraPassword",  # your password
                             db="topik")        # name of the data base
		
		curP = db.cursor()
		curT = db.cursor()
		curPop = db.cursor()
		
		curP.execute("SELECT * from posts;")
		curT.execute("CREATE OR REPLACE TABLE temp_distScore(score DOUBLE,uid NVARCHAR(20),pid NVARCHAR(20) PRIMARY KEY) engine=InnoDB;")
		db.commit()
		
		rows = curP.fetchall()
		
		for row in rows:
			
			q_l = (self.lat,self.lng)
			row_l = (row[3],row[4])
			
			dist = vincenty(q_l, row_l).miles
			
			if(self.r >= dist):
				distScore = float(self.r-dist)/self.r
			else:
				distScore = 0
			#	print distScore
			
			curT.execute("INSERT INTO temp_distScore (score,uid,pid) VALUES (%s,%s,%s)",(distScore,row[1],row[5]))
			
		
		
		db.commit()
		curP.close()
		curT.close()
		db.close()


query = Query("forget women",41,-72,100)

query.processQuery()

#query.showProcessedText()

query.findTopTextResults()
query.findTopDistResults()
