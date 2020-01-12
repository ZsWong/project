import os
import json

import secretary
import cleaner
import engineer

import pandas as pd
strZipsDir = "/home/zswong/workspace/data/zips"
strJobsDir = "/home/zswong/workspace/station_code/jobs"        
strJobsSamplesDir = "/home/zswong/workspace/station_code/samples"
strJsonFile = "/home/zswong/workspace/project/features.json"
with open(strJsonFile, "r") as f:
        mapSectionsAndFeatures = json.load(f)

strParts = list(mapSectionsAndFeatures.keys())
mapBinaryFeaturesAndValidNumber = {"DEMOD_CARRIERLOCK": 2, "DPU_FRAMESYNCSTATUS1": 2, "DPU_FRAMESYNCSTATUS2": 2}
strTimeDependentFeatures = {"counter": ["DPU_ERRORBITNUMBER1", "DPU_TOTALBITNUMBER1"]}

#secretary.fn_buildJobsDirFromZipsDir(strZipsDir, strJobsDir)
for name in os.listdir(strJobsDir):
        strJobDir = os.path.join(strJobsDir, name)
        engineer.fn_regularBoolFeaturesOfAJob(mapBinaryFeaturesAndValidNumber, strJobDir)
        engineer.fn_cullTimeEffectOfAJob(strTimeDependentFeatures, strJobDir)
        engineer.fn_extractValidRecordsOfAJob(strJobDir)
        engineer.fn_constructSectionsOfAJob(mapSectionsAndFeatures, strParts, strJobDir)
        engineer.fn_generateSamplesFromAJob(strJobDir)
secretary.fn_collectSamplesFromJobs(strJobsDir, strJobsSamplesDir)