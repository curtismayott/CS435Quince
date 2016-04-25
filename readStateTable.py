#!/usr/bin/env python3
#readStateTable.py

#author:Nikolas Szejna


import csv
CSV = 'state_table.csv'

def readCSV(csvName):
	with open(csvName, 'r') as f:
		reader = csv.reader(f)
		your_list = list(reader)
		return your_list

def findStringInList(searchString,list):
	stateFirstLetterCapital = searchString.lower().title() #make sure only first letter capital
	if(searchString in list):
		return list[0] # always returns the 1st index to get full state name
	elif(searchString.upper() in list): # example: check if user typed 'co' and list has 'CO'
		return list[0]
	elif(stateFirstLetterCapital in list): # example: check if user typed 'colorado' and list has 'Colorado'
		return list[0]
	else:
		return None

def findStringInSubList(string,list): #list is a list of [State,abbrev.] pairs
	for sub in list:
		word = findStringInList(string,sub)
		if(word is not None):
			return word
	return None

def getCorrespondingState(stateString,csvName):
	stateList = readCSV(csvName)
	state = findStringInSubList(stateString,stateList)
	return state # returns None if not found


