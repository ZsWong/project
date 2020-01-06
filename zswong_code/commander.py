import os

import secretary
import cleaner
import engineer

import pandas as pd
strRawStatusFile = "/home/zswong/workspace/station_code/jobs/JOB201912170654200/Demod1/raw_status.csv"
pdDfRawStatus = pd.read_csv(strRawStatusFile)
pdDfSeries = pdDfRawStatus.loc[:, "DPU_BER1"]
print(pdDfSeries[251: 270])

strZipsDir = "/home/zswong/workspace/data/zips"
strJobsDir = "/home/zswong/workspace/station_code/jobs"        

mapSectionsFeatures = {"input": ["DEMOD_IFLEVEL", "DEMOD_EBNOVALUE", "DEMOD_EBNOVALUEQCHL"],
"carrierlock": ["DEMOD_CARRIERLOCK", "DEMOD_CARRIEROFFSET"],
"bitlock": ["DEMOD_BITLOCK", "DEMOD_BITLOCKQCHL", "DEMOD_BITRATEOFFSET"]
}
strParts = ["input", "carrierlock", "bitlock"]
mapBinaryFeaturesAndValidNumber = {"DEMOD_CARRIERLOCK": 2, "DPU_FRAMESYNCSTATUS1": 2, "DPU_FRAMESYNCSTATUS2": 2}
strTimeDependentFeatures = ["DPU_ERRORBITNUMBER1", "DPU_TOTALBITNUMBER1"]

secretary.fn_buildJobsDirFromZipsDir(strZipsDir, strJobsDir)
for name in os.listdir(strJobsDir):
        strJobDir = os.path.join(strJobsDir, name)
        engineer.fn_regularBoolFeaturesOfAJob(mapBinaryFeaturesAndValidNumber, strJobDir)
        engineer.fn_cullTimeEffectOfAJob(strTimeDependentFeatures, strJobDir)
        engineer.fn_extractValidRecordsOfAJob(strJobDir)
        engineer.fn_constructSectionsOfAJob(mapSectionsFeatures, strParts, strJobDir)
        engineer.fn_generateSamplesFromAJob(strJobDir)