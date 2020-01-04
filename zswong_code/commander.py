import os

import secretary
import cleaner
import engineer

strZipsDir = "/home/zswong/workspace/data/zips"
strJobsDir = "/home/zswong/workspace/station_code/jobs"        

mapSectionsFeatures = {"input": ["DEMOD_IFLEVEL", "DEMOD_EBNOVALUE", "DEMOD_EBNOVALUEQCHL"],
"carrierlock": ["DEMOD_CARRIERLOCK", "DEMOD_CARRIEROFFSET"],
"bitlock": ["DEMOD_BITLOCK", "DEMOD_BITLOCKQCHL", "DEMOD_BITRATEOFFSET"]
}
strParts = ["input", "carrierlock", "bitlock"]
mapBinaryFeaturesAndValidNumber = {"DEMOD_CARRIERLOCK": 2, "DPU_FRAMESYNCSTATUS1": 2, "DPU_FRAMESYNCSTATUS2": 2}

for name in os.listdir(strJobsDir):
        strJobDir = os.path.join(strJobsDir, name)
        engineer.fn_regularBoolFeaturesOfAJob(mapBinaryFeaturesAndValidNumber, strJobDir)
        engineer.fn_extractValidRecordsOfAJob(strJobDir)
        engineer.fn_constructSectionsOfAJob(mapSectionsFeatures, strParts, strJobDir)
        engineer.fn_generateSamplesFromAJob(strJobDir)