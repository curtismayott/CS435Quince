#!/usr/bin/env python3

#fileDown.py

#Author: Nikolas Szejna

#Purpose: download all of the PM2.5 FRM/FEM Mass (88101) hourly data from the epa website
#Years 1990-2015

import urllib.request,os
import zipfile
from os import listdir
from os.path import isfile, join
#import numpy as np

def unzip_file(pathToFile,destDirectory):
    zip_ref = zipfile.ZipFile(pathToFile, 'r')
    zip_ref.extractall(destDirectory)
    zip_ref.close()

def unzip_all_files(pathsList,destDirectory):
    for path in pathsList:
        unzip_file(path,destDirectory)

def listOfDirectoryFiles(directory):
    onlyfiles = [f for f in listdir(directory) if isfile(join(directory, f))]
    return onlyfiles

def concatenate_all_files(directory,outfilePath):
    fileList = listOfDirectoryFiles(directory)
    with open(outfilePath, 'w') as outfile:
        for fname in fileList:
            with open(FINAL_DIRECTORY+"/"+fname) as infile:
                for line in infile:
                    outfile.write(line)


    

NUMYEARS = 26

STARTYEAR = 1990

UNZIP_AFTER_DOWNLOAD = True

CONCAT_AFTER_UNZIP = True

FINAL_DIRECTORY = "hourly_88101"

OUTFILE_NAME = "hourly_88101.csv"


#url = "http://aqsdr1.epa.gov/aqsweb/aqstmp/airdata/hourly_88101_2015.zip"
urlStart = "http://aqsdr1.epa.gov/aqsweb/aqstmp/airdata/hourly_88101_"
urlsNames = [None]*NUMYEARS
fileNames = [None]*NUMYEARS


for num in range(NUMYEARS):
    year = STARTYEAR +num
    urlsNames[num] = urlStart+str(year)+".zip"

    url = urlsNames[num]
    file_name = url.split('/')[-1]
    fileNames[num] = file_name
    u = urllib.request.urlopen(url)
    f = open(file_name, 'wb')
    meta = u.info()
    #below is code that would be nice but doesn't work yet:
    #file_size = int(meta.getheaders("Content-Length")[0])
    #print("Downloading: %s Bytes: %s" % (file_name, file_size))
    #os.system('cls')
    file_size_dl = 0
    block_sz = 8192
    while True:
        buffer = u.read(block_sz)
        if not buffer:
            break

        file_size_dl += len(buffer)
        f.write(buffer)
        #below is code that would be nice but doesn't work yet:
        #status = r"%10d  [%3.2f%%]" % (file_size_dl, file_size_dl * 100. / file_size)
        #status = status + chr(8)*(len(status)+1)
        #print (status)

f.close()
print("finished downloading "+str(NUMYEARS)+" files")

if(UNZIP_AFTER_DOWNLOAD):
    if(not None in fileNames): # make sure all of the paths are valid
        unzip_all_files(fileNames,FINAL_DIRECTORY)

    print("finished unzipping "+str(len(fileNames))+" files")

    if(CONCAT_AFTER_UNZIP):
        concatenate_all_files(FINAL_DIRECTORY,FINAL_DIRECTORY+"/"+OUTFILE_NAME)
        print("finished concatenating all files")
