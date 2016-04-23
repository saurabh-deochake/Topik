import MySQLdb
from nltk.stem.snowball import SnowballStemmer
from nltk.corpus import stopwords
from geopy.distance import vincenty


class Query():
	def __init__(self, text, lat, lng,r):
		self.text = text
		self.w = 0.1
		self.popDepth = 5
		self.lat = lat
		self.lng = lng
		self.r = r
		
		return

	def processQuery(self):
		stop = stopwords.words('english')
		stemmer = SnowballStemmer("english")
        
		words = [i for i in self.text.split() if i not in stop]
		
		stemwords = []
		for w in words:
			stemwords.append(stemmer.stem(w))
        
		self.processedText = " ".join(stemwords)
		
		return

	def showProcessedText(self):
		print self.processedText
		
		return

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
				
				while i <= self.popDepth:
					tweet_ids.append([])
					for j in tweet_ids[i]:
						curPop.execute("SELECT pid FROM posts WHERE rpid = %s;",[j])
						tempList = []
						tempList.extend(list(curPop.fetchall()))
						if len(tempList) > 0:
							tweet_ids[i+1].append(tempList)
					i += 1
					scoreMultiplier += (float(1)/(i+1) * len(tweet_ids[i]))
			
			curT.execute("INSERT INTO temp_textScore (score,uid,pid) VALUES (%s,%s,%s)",(match*scoreMultiplier,row[1],row[5]))
		
		db.commit()
		curP.close()
		curT.close()
		db.close()
		
		return


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

			curT.execute("INSERT INTO temp_distScore (score,uid,pid) VALUES (%s,%s,%s)",(distScore,row[1],row[5]))

		db.commit()
		curP.close()
		curT.close()
		db.close()
		
		return
	
	def findTopK(self,kLim = 5, alpha = 0.5):
		#query.findTopTextResults()
		#query.findTopDistResults()
		
		LDist = []
		LText = []
		
		topKList = []
		
		raw_input("Press Enter to continue...")
		
		db = MySQLdb.connect(host="localhost",    # your host, usually localhost
                             user="spd",         # your username
                             passwd="meraPassword",  # your password
                             db="topik")        # name of the data base
		
		
		curDist = db.cursor()
		curText = db.cursor()
		
		curDist.execute("SELECT uid,SUM(score) FROM temp_distScore GROUP BY uid HAVING SUM(score) >0 ORDER BY sum(score) DESC;")
		curText.execute("SELECT uid,SUM(score) FROM temp_textScore GROUP BY uid HAVING SUM(score) >0 ORDER BY sum(score) DESC;")
		
		LDist.extend(list(curDist.fetchall()))
		LText.extend(list(curText.fetchall()))
		
		k = 0
		i = 0
		buff = []
		
		raw_input("Press Enter to continue...")
		for i in range(len(LDist)):
			UserL1 = LDist[i][0]
			ScoreL1 = LDist[i][1]
			
			UserL2 = LText[i][0]
			ScoreL2 = LText[i][1]
			
			threshold = alpha*ScoreL1 + (1-alpha)*ScoreL2
			
			if UserL1 == UserL2:
				scoreLB = threshold
				scoreUB = threshold
				buff.append({"user":UserL1, "scoreLB":scoreLB, "scoreUB":scoreUB,"LB" : 1})
			else:
				indexUserL1 = next((i for i, v in enumerate(buff) if v["user"] == UserL1), None)
				if(indexUserL1):
					buff[indexUserL1]["scoreLB"] += alpha*ScoreL1
					buff[indexUserL1]["scoreUB"] = buff[indexUserL1]["scoreLB"]
				else:
					buff.append({"user":UserL1, "scoreLB":alpha*ScoreL1, "scoreUB":threshold,"LB" : 1})
	
				indexUserL2 = next((i for i, v in enumerate(buff) if v["user"] == UserL2), None)
				if(indexUserL2):
					buff[indexUserL2]["scoreLB"] += (1-alpha)*ScoreL2
					buff[indexUserL2]["scoreUB"] = buff[indexUserL2]["scoreLB"]
				else:
					buff.append({"user":UserL2, "scoreLB":(1-alpha)*ScoreL2, "scoreUB":threshold,"LB" : 2})
			
			print buff
			raw_input("Press Enter to continue...Before Update")
			print threshold
			
			#Update buffers
			for b in buff:
				if (b["LB"]==1):
					b["scoreUB"] = b["scoreLB"] + (1-alpha)*ScoreL2
				else:
					b["scoreUB"] = b["scoreLB"] + alpha*ScoreL1
				
			tempBuff = []
			for b in buff:
				if (b["scoreLB"]>threshold):
					print "This element is being considered: ", b
					topKList.append(b)
					k += 1
				else:
					tempBuff.append(b)
			
			buff = tempBuff
			
			if(k>=kLim):
				return topKList
			print buff
			raw_input("Press Enter to continue...After Update")
		return topKList

query = Query("forget women",41,-72,100)

query.processQuery()

#query.showProcessedText()

#query.findTopTextResults()
#query.findTopDistResults()

k=5
alpha = 0.5
print query.findTopK(k,alpha)
