import MySQLdb
from nltk.stem.snowball import SnowballStemmer
from nltk.corpus import stopwords

class Query():
	def __init__(self, text = None):
		self.text = text

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
		
		curP.execute("SELECT * from posts;")
		curT.execute("CREATE OR REPLACE TABLE temp_textScore(score INT(10),uid NVARCHAR(20),pid NVARCHAR(20) PRIMARY KEY) engine=InnoDB;")
		db.commit()
		
		while True:
			row = curP.fetchone()
			match = 0
			if(row == None):
				break
			for word in row[2].split():
				if word in self.processedText.split():
					match +=1
			curT.execute("INSERT INTO temp_textScore (score,uid,pid) VALUES (%s,%s,%s)",(match,row[1],row[5]))
		
		
		db.commit()
		curP.close()
		curT.close()
		db.close()


query = Query("forget women")

query.processQuery()

query.showProcessedText()

query.findTopTextResults()
