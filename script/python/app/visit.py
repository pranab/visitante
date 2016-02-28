#!/usr/bin/python

import os
import sys
from random import randint
import random
import time
import datetime
import uuid
import threading
sys.path.append(os.path.abspath("../lib"))
from util import *
from sampler import *

custCount = int(sys.argv[1])
monthsInPast = int(sys.argv[2])
visitCustPercent = int(sys.argv[3])
avRetentionPeriodDay = int(sys.argv[4]) * 30

customers = []
custLifeCycleEnd = {}

minPerDay = 24 * 60
secPerDay = minPerDay * 60

now = datetime.datetime.now()
nowEpoch = int(now.strftime("%s"))
daysInPast = monthsInPast * 30
past = now - datetime.timedelta(days=daysInPast)
pastDateEpoch = int(past.strftime("%s"))
retentionDistr = GaussianRejectSampler(avRetentionPeriodDay, 30)

for i in range(0,custCount):
	custID = genID(10)
	customers.append(custID)
	retentionFrac = 0.5 *  (random.random() + 1.0)
	endTime = pastDateEpoch +  int(retentionFrac * retentionDistr.sample() * secPerDay)
	custLifeCycleEnd[custID] = endTime
	

dailyVisitCount = int((custCount * visitCustPercent) / 100)
visitIntervalMin = minPerDay / dailyVisitCount
#print "%d,%d"  %(dailyVisitCount, visitIntervalMin)
#print past.strftime("%Y-%m-%d %H:%M:%S")

visitDate = past
visitDateEpoch = pastDateEpoch
acqCust = 0
visits = 0
while (visitDateEpoch < nowEpoch):
	visits = visits + 1
	if(randint(0,100) < 15):
	    #new customer
		custID = genID(10)
		customers.append(custID)
		custLifeCycleEnd[custID] = visitDateEpoch + retentionDistr.sample() * secPerDay
		acqCust = acqCust + 1
	else:
		#existing customer
		while(True):
			custID = selectRandomFromList(customers)
			endTime = custLifeCycleEnd[custID]
			if (endTime < visitDateEpoch):
				#lost customer
				customers.remove(custID)
				continue
			else:
				break
		
	visitDateStr = visitDate.strftime("%Y-%m-%d %H:%M:%S")
	print "%s,%s" %(custID, visitDateStr)
	
	intervalMin = int(visitIntervalMin + (random.random() - 0.5) * visitIntervalMin * 0.3)
	visitDate = visitDate + datetime.timedelta(minutes=intervalMin)		
	visitDateEpoch = int(visitDate.strftime("%s"))

#lostCust = 0
#for custID in custLifeCycleEnd:
#	if (custLifeCycleEnd[custID] < nowEpoch):
#		lostCust = lostCust + 1
#print "%d,%d,%d" %(visits,acqCust,lostCust)

