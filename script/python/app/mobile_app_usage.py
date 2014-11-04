#!/usr/bin/python

import os
import sys
from random import randint
import time
import redis
import uuid
import threading
sys.path.append(os.path.abspath("../lib"))
from util import *

areaCodes = [408, 650, 415, 212, 213, 248, 310, 339]
rc = redis.StrictRedis(host='localhost', port=6379, db=0)
phoneNumbers = []

def genLogs(threadName, areaCode, phoneNum):
	session = str(uuid.uuid1())

	numAccess = randint(4, 12)
	for i in range(numAccess):
		dt = time.strftime("%Y-%m-%d")
		tm = time.strftime("%H:%M:%S")
		log = "%s %s %s %d %s" %(dt, tm, session, areaCode, phoneNum)	
		print log
		#rc.lpush("logQueue", log)
		time.sleep(randint(1,4))
		
#command processing
op = sys.argv[1]
if (op == "genLogs"):	            
	#start multiple session threads
	numSession = int(sys.argv[2])
	try:
		for i in range(numSession):
			threadName = "user session-%d" %(i)
			
			if (randint(0,100) < 70 or i < 5):
				areaCode = selectRandomFromList(areaCodes)
				exchCode = randint(100, 999)
				phNum = randint(1000, 9999)
				phoneNum = "(%d)%d%d" %(areaCode, exchCode, phNum)
				phoneNumbers.append(phoneNum)
				#print "new phone num"
			else:
				phoneNum = selectRandomFromList(phoneNumbers)
				areaCode = int(phoneNum[1:4])
				#print "existing phone num"
				
			t = threading.Thread(target=genLogs, args=(threadName, areaCode, phoneNum ))
   			t.start()
			time.sleep(randint(8,12))
	except:
   		print "Error: unable to start thread"
			


		
