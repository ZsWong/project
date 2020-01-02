import os

import secretary
import cleaner
import engineer

strZipsDir = "/home/zswong/workspace/data/zips"
strJobsDir = "/home/zswong/workspace/station_code/jobs"        
strParts = ["input", "carrierlock", "bitlock"]
mapNamePart = {"input": ["DEMOD_IFLEVEL", "DEMOD_EBNOVALUE", "DEMOD_EBNOVALUEQCHL"],
        "carrierlock": ["DEMOD_CARRIERLOCK", "DEMOD_CARRIEROFFSET"],
        "bitlock": ["DEMOD_BITLOCK", "DEMOD_BITLOCKQCHL", "DEMOD_BITRATEOFFSET"]}

strParts = ["input", "carrierlock", "bitlock"]
strBinaryFeatures = ["DEMOD_CARRIERLOCK", "DEMOD_BITLOCK", "DEMOD_BITLOCKQCHL"]


for name in os.listdir(strJobsDir):
        strJobDir = os.path.join(strJobsDir, name)
        engineer.fn_extractValidRecordsOfAJob(strJobDir)
        engineer.fn_constructSectionsOfAJob(mapNamePart, strParts, strBinaryFeatures, strJobDir)
        engineer.fn_generateSamplesFromAJob(10, strJobDir)