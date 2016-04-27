#!/usr/bin/env python3

#extractCounty

#Author: Nikolas Szejna

#Purpose: get a county out of epa dataset
#Years 1990-2015

import urllib.request,os

from os import listdir
from os.path import isfile, join

def listOfDirectoryFiles(directory):
    onlyfiles = [f for f in listdir(directory) if isfile(join(directory, f))]
    return onlyfiles

def concatenate_all_files_county(directory,outfilePath,county):
    fileList = listOfDirectoryFiles(directory)
    with open(outfilePath, 'w') as outfile:
        for fname in fileList:
            with open(FINAL_DIRECTORY+"/"+fname) as infile:
                for line in infile:
                    if "Trinity" in line:
                        outfile.write(line)


    

COUNTY = "Fresno"

FINAL_DIRECTORY = "hourly_88101"

OUTFILE_NAME = "hourly_88101_"+COUNTY+".csv"


concatenate_all_files_county(FINAL_DIRECTORY,FINAL_DIRECTORY+"/"+OUTFILE_NAME,COUNTY)
print("finished concatenating all files containing "+ COUNTY)
