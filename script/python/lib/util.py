#!/usr/bin/python

import sys
from random import randint
import time
import uuid

tokens = ["0","1","2","3","4","5","6","7","8","9","A","B","C","D","E","F","G","H","I","J","K","L","M",
	"N","O","P","Q","R","S","T","U","V","W","X","Y","Z","0","1","2","3","4","5","6","7","8","9"]

def genID(len):
	id = ""
	for i in range(len):
		id = id + selectRandomFromList(tokens)
	return id
		
		
def selectRandomFromList(list):
	return list[randint(0, len(list)-1)]
	
def selectRandomSubListFromList(list, num):
	sel = selectRandomFromList(list)
	selSet = {sel}
	selList = [sel]
	while (len(selSet) < num):
		sel = selectRandomFromList(list)
		if (sel not in selSet):
			selSet.add(sel)
			selList.append(sel)		
	return selList