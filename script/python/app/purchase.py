#!/usr/bin/python

import os
import sys
from random import randint
from random import random
import time
import datetime
import uuid
import threading
sys.path.append(os.path.abspath("../lib"))
from util import *

custCount = int(sys.argv[1])
buyCount = int(sys.argv[2])
buyingCustPercent = int(sys.argv[3])
avXactionAmount = float(sys.argv[4])

hiValueCustCount = custCount / 10
hiValueCustAvXactionAmount = avXactionAmount * 2.0

customers = []
hiValueCustomers = []
minPerDay = 24 * 60

for i in range(0,custCount):
	customers.append(genID(10))
	
for i in range(0,hiValueCustCount):
	hiValueCustomers.append(genID(10))

dailyBuyCount = int((custCount * buyingCustPercent) / 100)
buyIntervalMin = minPerDay / dailyBuyCount
#print "%d,%d"  %(dailyBuyCount, buyIntervalMin)

daysInPast = (buyCount * buyIntervalMin) /  minPerDay + 2
now = datetime.datetime.now()
past = now - datetime.timedelta(days=daysInPast)
#print past.strftime("%Y-%m-%d %H:%M:%S")

xactionDate = past
for i in range(0, buyCount):
	xactID = genID(14)
	if(randint(0,100) < 10):
		custID = selectRandomFromList(hiValueCustomers)
		if (randint(0,100) < 10):
			xactionAmount = hiValueCustAvXactionAmount +  random() * (2.0 * hiValueCustAvXactionAmount)
		else:
			xactionAmount = hiValueCustAvXactionAmount +  random() * (0.3 * hiValueCustAvXactionAmount)
	else:
		custID = selectRandomFromList(customers)
		if (randint(0,100) < 10):
			xactionAmount = hiValueCustAvXactionAmount +  random() * (2.0 * avXactionAmount)
		else:
			xactionAmount = avXactionAmount +  random() * (0.5 * avXactionAmount)
		
	intervalMin = int(buyIntervalMin + (random() - 0.5) * buyIntervalMin * 0.2)
	xactionDate = xactionDate + datetime.timedelta(minutes=intervalMin)		
	xactionDateStr = 	xactionDate.strftime("%Y-%m-%d %H:%M:%S")
	
	print "%s,%s,%s,%.2f" %(xactID, custID, xactionDateStr, xactionAmount)
	
	
	
	

