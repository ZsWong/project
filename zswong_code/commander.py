import os

import secretary
import cleaner
import engineer

g_strJobsDir = "/home/zswong/workspace/data/jobs"        
strParts = ["input", "carrierlock", "bitlock"]
mapNamePart = {"input": ["DEMOD_IFLEVEL", "DEMOD_EBNOVALUE", "DEMOD_EBNOVALUEQCHL"],
        "carrierlock": ["DEMOD_CARRIERLOCK", "DEMOD_CARRIEROFFSET"],
        "bitlock": ["DEMOD_BITLOCK", "DEMOD_BITLOCKQCHL", "DEMOD_BITRATEOFFSET"],
        "framesync": ["DPU_FRAMESYNCSTATUS1", "DPU_FRAMESYNCSTATUS2"]}

strParts = ["input", "carrierlock", "bitlock", "framesync"]
strBinaryFeatures = ["DEMOD_CARRIERLOCK", "DEMOD_BITLOCK", "DEMOD_BITLOCKQCHL", 
"DPU_FRAMESYNCSTATUS1", "DPU_FRAMESYNCSTATUS2"]


for name in os.listdir(g_strJobsDir):
        strJobDir = os.path.join(g_strJobsDir, name)
        engineer.fn_constructSectionsOfAJob(mapNamePart, strParts, strBinaryFeatures, strJobDir)
        engineer.fn_generateSamplesFromAJob(10, strJobDir)