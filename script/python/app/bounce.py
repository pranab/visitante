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

pageCountMax = 15
items = []
pagesGET = ["/shoppingCart","/product","/onSale","/brands", "/about"]
pagesPOST = ["/addToCart","/checkOut","/billing","/confirmShipping","/placeOrder"]
homePages = ["home1", "home2"]
rc = redis.StrictRedis(host='localhost', port=6379, db=0)
epochInterval = 10

#Fields: date time c-ip   cs-method cs-uri-stem  sc-status   cs(User-Agent) cs(Referrer) 
#2002-05-24 20:18:01 172.224.24.114  GET /Default.htm - 200  Mozilla/4.0+(compatible;+MSIE+5.01;+Windows+2000+Server) http://64.224.24.114/

def genLogs(threadName, items, maxPages, pagesGET, pagesPOST, homePages):
	numItemsForThisUser = randint(10, 20)
	itemsForThisUser = selectRandomSubListFromList(items, numItemsForThisUser)
	session = str(uuid.uuid1())
	ipAddress = genIpAddress()
	status = 200
	
	method = "GET"
	page = "/" + selectRandomFromList(homePages) + "?sessionid=" + session
	createLog(page, ipAddress,  method, status)
	if (maxPages == 1):
		return
	time.sleep(randint(4,8))
	
	if(randint(0,10) < 4):
		#all GET
		for i in range(maxPages):
			page = ""
			method = "GET"
			select = randint(0,10)
			if (select < 6):
				page = "/product?sessionid=" + session + "&productid=" + selectRandomFromList(itemsForThisUser)
			elif (select < 8):
				page = "/onSale?sessionid=" + session
			elif (select < 9):
				page = "/brands?sessionid=" + session
			else:
				page = "/about?sessionid=" + session
			
			createLog(page, ipAddress,  method, status)
			time.sleep(randint(4,8))
			
			if (randint(0,10) < 3):
				page = "/signup?sessionid=" + session
				createLog(page, ipAddress,  method, status)
				time.sleep(randint(4,8))
	
	else:
		#mixed
		firstInPost = False
		signedIn = False
		for i in range(maxPages):
			page = ""
			if (randint(0,10) < 5):
				method = "GET"
				page = selectRandomFromList(pagesGET)
			else:	
				method = "POST"
				if (not firstInPost):
					if((not signedIn) and randint(0,10) < 7):
						page = "/signin"
						signedIn = True
					else:
						page = selectRandomFromList(pagesPOST)
					firstInPost = True
				else:
					page = selectRandomFromList(pagesPOST)
					
			page = page + "?sessionid=" + session
			if(page.find("product") >= 0):
				page = page + "&productid=" + selectRandomFromList(itemsForThisUser)
			createLog(page, ipAddress,  method, status)
			time.sleep(randint(4,8))
			
			if (signedIn and randint(0,10) < 7):
				page = "/logout"
				page = page + "?sessionid=" + session
				method = "POST"
				createLog(page, ipAddress,  method, status)
	

def createLog(page, ipAddress,  method, status):
	dt = time.strftime("%Y-%m-%d")
	tm = time.strftime("%H:%M:%S")
	log = "%s %s %s %s %s %d" %(dt, tm, ipAddress, method, page, status)	
	print log
	rc.lpush("logQueue", log)

def readLogQueue():
	while True:
		line = rc.rpop("logQueue")
		if line is not None:
			print line
		else:
			break

def readStatQueue():
	while True:
		line = rc.rpop("visitStatQueue")
		if line is not None:
			print line
		else:
			break

#command processing
op = sys.argv[1]
if (op == "genLogs"):	            
	#start multiple session threads
	numItems = int(sys.argv[2])
	numSession = int(sys.argv[3])
	print "numItems %d numSession %d" %(numItems, numSession)

	#generate item IDs
	for i in range(numItems):
		id = genID(8)
		items.append(id)

	try:
		singlePageSessionPercent = 10
		for i in range(numSession):
			threadName = "user session-%d" %(i)
			
			if (i % 10 == 0):
				singlePageSessionPercent = randint(10,20)
			
			if (randint(0,100) < singlePageSessionPercent):
				maxPage = 1
			else:
				maxPage = randint(2, pageCountMax)
   			t = threading.Thread(target=genLogs, args=(threadName, items,maxPage,pagesGET,pagesPOST,homePages ))
   			t.start()
			time.sleep(randint(6,12))

	except:
   		print "Error: unable to start thread"

elif (op == "readLogQueue"):
	readLogQueue()

elif (op == "readStatQueue"):
	readStatQueue()

