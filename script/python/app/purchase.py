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
hiValueCustAvXactionAmount = avXactionAmount * 2.5

customers = []
hiValueCustomers = []
churningCustomers = set()
mostValuableCustomers = set()

minPerDay = 24 * 60

for i in range(0,custCount):
	customers.append(genID(10))
	
for i in range(0,hiValueCustCount):
	hiValueCustomers.append(genID(10))

for i in range(0,5):
	churningCustomers.add(selectRandomFromList(customers))
#print churningCustomers

for i in range(0,5):
	mostValuableCustomers.add(selectRandomFromList(hiValueCustomers))

dailyBuyCount = int((custCount * buyingCustPercent) / 100)
buyIntervalMin = minPerDay / dailyBuyCount
#print "%d,%d"  %(dailyBuyCount, buyIntervalMin)

daysInPast = (buyCount * buyIntervalMin) /  minPerDay + 2
now = datetime.datetime.now()
past = now - datetime.timedelta(days=daysInPast)
#print past.strftime("%Y-%m-%d %H:%M:%S")

xactionDate = past
xactionDateEpoch = int(xactionDate.strftime("%s"))
nowEpoch = int(now.strftime("%s"))
idleTime = 30 * 24 * 60 * 60
cust = set()
mostValCust = set()

while (xactionDateEpoch < nowEpoch):
	xactID = genID(14)
	churning = False;
	if(randint(0,100) < 10):
		custID = selectRandomFromList(hiValueCustomers)
		if (randint(0,100) < 10):
			xactionAmount = hiValueCustAvXactionAmount +  random() * (2.5 * hiValueCustAvXactionAmount)
		else:
			xactionAmount = hiValueCustAvXactionAmount +  random() * (0.3 * hiValueCustAvXactionAmount)

		#most valuable customers
		mostValCust.clear()
		mostValCust.add(custID)
		if (mostValuableCustomers.issuperset(mostValCust)):
			xactionAmount = xactionAmount + 50 * (1.5 + random())
	else:
		custID = selectRandomFromList(customers)
		if (randint(0,100) < 10):
			xactionAmount = avXactionAmount +  random() * (2.0 * avXactionAmount)
		else:
			xactionAmount = avXactionAmount +  random() * (0.5 * avXactionAmount)
		
		#churning customers
		cust.clear()
		cust.add(custID)
		if (((nowEpoch - xactionDateEpoch) <  idleTime) and (churningCustomers.issuperset(cust))):
			churning = True;
			#print "churning %s" %(custID)
		
	intervalMin = int(buyIntervalMin + (random() - 0.5) * buyIntervalMin * 0.2)
	xactionDateStr = xactionDate.strftime("%Y-%m-%d %H:%M:%S")
	if(not churning): 
		print "%s,%s,%s,%.2f" %(xactID, custID, xactionDateStr, xactionAmount)
	
	xactionDate = xactionDate + datetime.timedelta(minutes=intervalMin)		
	xactionDateEpoch = int(xactionDate.strftime("%s"))
	
	
	
	

