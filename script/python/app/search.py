#!/usr/bin/python

import os
import sys
from random import randint
import time
sys.path.append(os.path.abspath("../lib"))
from util import *
from sampler import *

def cliskDistr(size):
	distr = []
	sum = 0
	for i in range(size):
		d = randomFloat(0, 0.95)
		distr.append(d)
		sum += d
		
	t = [d/sum for d in distr]
	distr = t
	distr.sort(reverse = True)
	
	#mutate
	if (isEventSampled(50)):
		first = randint(0,3)
		second = first + randint(1,3)
		swap(distr, first, second)
		
	return distr			

def buildClickDistr(distr):
	dvalues = []
	for i, v in enumerate(distr):
		iv = int(1000 * v)
		t = (i,iv)
		#print t
		dvalues.append(t)
	return CategoricalRejectSampler(dvalues)
			
op = sys.argv[1]

if op == "score":
	numQuery = int(sys.argv[2])
	numDoc = int(sys.argv[3])
	numDocPerQuery = int(sys.argv[4])
	docs = genIdList(numDoc, 8)
	for i in range(numQuery):
		quId = genID(12)
		scores = []
		for j in range(numDocPerQuery):
			scores.append(randomFloat(0.30, 0.95))
		scores.sort(reverse = True)
		
		for j in range(numDocPerQuery):
			docId = selectRandomFromList(docs)
			score = scores[j]
			print "%s,%s,%.3f" %(quId, docId, score)
			
elif op == "qdist":
	scFile = sys.argv[2]
	numDocPerQuery = int(sys.argv[3])
	cuQuId = ""
	for rec in fileRecGen(scFile, ","):
		quId = rec[0]
		if not quId == cuQuId:
			cuQuId = quId
			distr = cliskDistr(numDocPerQuery)
			sdistr = ["%.3f" %(d) for d in distr]
			print quId + "," + ",".join(sdistr)
	
	
elif op == "rel":
	scFile = sys.argv[2]
	qdFile = sys.argv[3]
	numEvent = int(sys.argv[4])

	#doc scores
	cuQuId = ""
	quRes = {}
	queries = []
	for rec in fileRecGen(scFile, ","):
		quId =  rec[0]
		docId = rec[1]
		if quId == cuQuId:
			docs = quRes[cuQuId]
			docs.append(docId)
		else:
			cuQuId = quId
			docs = []
			docs.append(docId)
			quRes[cuQuId] = docs
			queries.append(quId)
	
	#click distr
	cdistr = {}
	for rec in fileRecGen(qdFile, ","):
		quId =  rec[0]
		dvalues = rec[1:]
		distr = [float(d) for d in dvalues]
		cdistr[quId] = buildClickDistr(distr)
	
	#events	
	sampIntv = 15
	curTime = int(time.time())
	pastTime = curTime - (numEvent + 10000) * sampIntv
	sampTime = pastTime
	sessions = {}
	numSession = 10
	sessTimeThreshold = 600
	
	for i in range(numEvent):
		#remove oldest session
		maxElapsedTm = 0
		for k, v in sessions.items():
			elapsedTm = sampTime - v[1]
			if (elapsedTm > maxElapsedTm):
				maxElapsedTm = elapsedTm
				oldestSess = k
		
		threshold = sampleFromBase(sessTimeThreshold, 300)
		if (maxElapsedTm > threshold):
			del sessions[oldestSess]	
			#print "removed session"
		
		#add user session
		numSessThreshold = sampleFromBase(numSession, 2)
		sessId = None
		if (len(sessions) < numSessThreshold):
			sessId = genID(16)
			quId = selectRandomFromList(queries)
			sessions[sessId] = (quId, sampTime)
			#print "added session"
		
		#click event
		if sessId is None:
			sessId = selectRandomFromList(sessions.keys())
		quId = sessions[sessId][0]
		dindex = cdistr[quId].sample()
		docId = quRes[quId][dindex]
		#print "doc index " + str(dindex)
		print "%s,%s,%d" %(quId, docId, 1)
		sampTime += sampleFromBase(sampIntv, 2)
		
		