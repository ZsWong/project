from parameter import * 
import pandas as pd
import numpy as np

import os
import shutil
import xml.etree.ElementTree as ET
from functools import partial

"""
Extract valid records of a job's status files.
After this function, the structure of JOBXXXXXX
will be like below:
JOBXXXXXXX
        raw/
                status.csv
        valid/
                status.csv
"""
def fn_extractValidRecordsOfJob(strJobDir):
        strWorkSchRepXMLFile = os.path.join(strJobDir, "WorkSch_TASK.xml")
        oETWorkSchRep = ET.parse(strWorkSchRepXMLFile)
        tuplePdTimestamps = fn_getValidPeriod(oETWorkSchRep)

        listStrDemodNames = [name for name in os.listdir(strJobDir) if "Demod" in name]
        for name in listStrDemodNames:
                strStatusDir = os.path.join(strJobDir, name)
                fn_extractValidRecordsOfRawStatus(tuplePdTimestamps[0],tuplePdTimestamps[1], strStatusDir)
def fn_extractValidRecordsOfRawStatus(pdTimestampStart, pdTimestampEnd, strStatusDir):
        strValidStatusDir = os.path.join(strStatusDir, "valid")
        if os.path.exists(strValidStatusDir):
                shutil.rmtree(strValidStatusDir)
        os.mkdir(strValidStatusDir)
        
        strRawStatusFile = os.path.join(strStatusDir, "raw/status.csv")
        pdDfRawStatus = pd.read_csv(strRawStatusFile)
        strpdSeriesFilter = pdDfRawStatus.loc[:, "RECTIME"]
        pdTimestampSeriesFilter = strpdSeriesFilter.apply(lambda t: pd.Timestamp(t))
        pdDfFiltered = pdDfRawStatus.loc[(pdTimestampSeriesFilter >= pdTimestampStart) & (pdTimestampSeriesFilter <= pdTimestampEnd), :]
        strValidStatusFile = os.path.join(strValidStatusDir, "status.csv")
        pdDfFiltered.to_csv(strValidStatusFile, index = False)
def fn_getValidPeriod(oETWorkSchRep):
         strReceivingStartTime = "./content/equipmentInfo/receivingStartTime"
         strReceivingEndTime = "./content/equipmentInfo/receivingEndTime"
         opdTimestampStart = pd.Timestamp(oETWorkSchRep.find(strReceivingStartTime).text)
         opdTimestampEnd = pd.Timestamp(oETWorkSchRep.find(strReceivingEndTime).text)
         return opdTimestampStart, opdTimestampEnd

"""
Construct empty parts directory in status directories
After calling this function, the structure of JOBXXXXXXXX
will be like below:
JOBXXXXXXXXX
        raw/
                status.csv
        valid/
                status.csv
        parts/
"""
def fn_constructPartsOfJob(strJobDir):
        listStrDemodNames = [name for name in os.listdir(strJobDir) if "Demod" in name]
        for name in listStrDemodNames:
                strStatusDir = os.path.join(strJobDir, name)
                fn_constructPartsOfStatus(strStatusDir)
def fn_constructPartsOfStatus(strStatusDir):
        strPartsDir = os.path.join(strStatusDir, "parts")
        if os.path.exists(strPartsDir):
                shutil.rmtree(strPartsDir)
        os.mkdir(strPartsDir)


def fn_constructSectionOfJob(listStrFeatures, strPart, strSection, strJobDir):
        listStrDemodNames = [name for name in os.listdir(strJobDir) if "Demod" in name]
        for name in listStrDemodNames:
                strStatusDir = os.path.join(strJobDir, name)
                fn_constructSectionOfStatus(listStrFeatures, strPart, strSection, strStatusDir)
def fn_constructSectionOfStatus(listStrFeatures, strPart, strSection, strStatusDir):
        # To make sure that part directory is existing.
        strPartDir = os.path.join(strStatusDir + "/parts", strPart)
        if not os.path.exists(strPartDir):
                os.mkdir(strPartDir)
        # To clean up section directory.
        strSectionDir = os.path.join(strPartDir, strSection)
        if os.path.exists(strSectionDir):
                shutil.rmtree(strSectionDir)
        os.mkdir(strSectionDir)

        # Filter out features contained in the section.
        strValidStatusFile = os.path.join(strStatusDir, "valid/status.csv")
        print(strValidStatusFile)
        pdDfValidStatus = pd.read_csv(strValidStatusFile)
        pdDfSectionStatus = pdDfValidStatus[listStrFeatures]
        strSectionStatusFile = os.path.join(strSectionDir, "status.csv")
        pdDfSectionStatus.to_csv(strSectionStatusFile, index = False)

def fn_regularBoolFeaturesOfAJob(mapBinaryFeaturesAndValidNumber, strJobDir):
        for name in os.listdir(strJobDir):
                if "Demod" in name:
                        strStatusDir = os.path.join(strJobDir, name)
                        fn_regularBoolFeaturesOfAStatus(mapBinaryFeaturesAndValidNumber, strStatusDir)
def fn_regularBoolFeaturesOfAStatus(mapBinaryFeaturesAndValidNumber, strStatusDir):
        strRawStatusFile = os.path.join(strStatusDir, "raw_status.csv")
        pdDfRawStatus = pd.read_csv(strRawStatusFile)
        for feature, number in mapBinaryFeaturesAndValidNumber.items():
                pdSeries = pdDfRawStatus.loc[:, feature]
                pdSeriesResult = pd.Series(np.zeros(pdSeries.size))
                pdSeriesResult[pdSeries == number] = 1
                pdDfRawStatus.loc[:, feature] = pdSeriesResult
        strRegStatusFile = os.path.join(strStatusDir, "reg_status.csv")
        pdDfRawStatus.to_csv(strRegStatusFile, index = False)

def fn_cullTimeEffectOfAJob(mapTypesAndTimeDepentFeatures, strJobDir):
        for name in os.listdir(strJobDir):
                if "Demod" in name:
                        strStatusDir = os.path.join(strJobDir, name)
                        fn_cullTimeEffectOfAStatus(mapTypesAndTimeDepentFeatures, strStatusDir)
def fn_cullTimeEffectOfAStatus(mapTypesAndTimeDepentFeatures, strStatusDir):
        strRegStatusFile = os.path.join(strStatusDir, "reg_status.csv")
        pdDfRegStatus = pd.read_csv(strRegStatusFile)
        pdDfCulledStatus = fn_countBadFrameInOneSec(mapTypesAndTimeDepentFeatures["counter"], pdDfRegStatus)
        strCulledStatusFile = os.path.join(strStatusDir, "culled_status.csv")
        pdDfCulledStatus.to_csv(strCulledStatusFile, index = False)
def fn_countBadFrameInOneSec(strCounterFeatures, pdDfRegStatus):
        pdDfBadFrame = pdDfRegStatus.loc[:, strCounterFeatures]
        pdDfBadFrameCnted = pdDfBadFrame.rolling(2).apply(fn_sub)
        pdDfRegStatus.loc[:,strCounterFeatures] = pdDfBadFrameCnted
        return pdDfRegStatus
def fn_sub(nWin):
        if nWin.size == 1:
                return nWin
        elif nWin[1] < nWin[0]:
                return nWin[1]
        else:
                return nWin[1] - nWin[0]
"""
It's better to contain all records.
So there is no need in getting the number of samples as
it's input parameter.
"""
"""
callable objec为t
"""
def fn_isAllTrue(bnpNArr):
        for  b in bnpNArr:
                if not b:
                        return False
        return True
def fn_generateSamplesFromAJob(strJobDir):
        strSamplesDir = os.path.join(strJobDir, "samples")
        if os.path.exists(strSamplesDir):
                shutil.rmtree(strSamplesDir)
        strSamplesSectionsDir = os.path.join(strSamplesDir, "sections")
        os.makedirs(strSamplesSectionsDir)
        """
        name is like "Demodx"
        """
        strSections = [name for name in os.listdir(strJobDir) if "Demod" in name]
        for section in strSections:
                strStatusDir = os.path.join(strJobDir, section)
                strValStatusFile = os.path.join(strStatusDir, "val_status.csv")
                pdDfValStatus = pd.read_csv(strValStatusFile)
                bnpNArrFilter = (pdDfValStatus.loc[:, ["DPU_FRAMESYNCSTATUS1", "DPU_FRAMESYNCSTATUS2"]] == 1).values
                bnpNArrFilter = np.apply_along_axis(fn_isAllTrue, 1, bnpNArrFilter)
                fn_generateSamplesOfAStatus(bnpNArrFilter, strStatusDir, strSamplesSectionsDir)
def fn_generateSamplesOfAStatus(bnpNArrFilter, strStatusDir, strSamplesSectionsDir):
        strSectionsDir = os.path.join(strStatusDir, "sections")
        for name in os.listdir(strSectionsDir):
                strSectionDir = os.path.join(strSectionsDir, name)
                strSamplesSectionDir= os.path.join(strSamplesSectionsDir, name)
                if not os.path.exists(strSamplesSectionDir):
                        os.mkdir(strSamplesSectionDir)
                fn_generateSamplesFromASection(bnpNArrFilter, strSectionDir, strSamplesSectionDir)
def fn_generateSamplesFromASection(bnpNArrFilter, strSectionDir, strSamplesDir):
        strSectionFile = os.path.join(strSectionDir, "status.csv")
        pdDfStatus = pd.read_csv(strSectionFile)

        strTrainingSamplesFile = os.path.join(strSamplesDir, "samples.csv")
        pdDfTrainingSamples = pdDfStatus.loc[bnpNArrFilter, :]
        if os.path.exists(strTrainingSamplesFile):
                pdDfFormerTrainingSamples = pd.read_csv(strTrainingSamplesFile)
                pdDfTrainingSamples  = pd.concat([pdDfFormerTrainingSamples, pdDfTrainingSamples], axis=0)
        pdDfTrainingSamples.to_csv(strTrainingSamplesFile, index = False)

        strNegativeSamplesFile = os.path.join(strSectionDir, "samples.csv")
        pdDfNegtiveSamples = pdDfStatus.loc[~bnpNArrFilter, :]
        pdDfNegtiveSamples.to_csv(strNegativeSamplesFile, index = False)
                
if __name__ == "__main__":
        strJobsDir = "d:/docker_for_winter/jobs"
        
        for name in os.listdir(strJobsDir):
                strJobDir = os.path.join(strJobsDir, name)
                fn_constructSectionOfJob(["DEMOD_IFLEVEL", "DEMOD_EBNOVALUE", "DEMOD_EBNOVALUEQCHL"], 
        "framelock", "input", strJobDir)
        """
        for job in os.listdir(strJobsDir):
                strJobDir = os.path.join(strJobsDir, job)
                fn_constructPartsOfJob(strJobDir)
        """
        """
        mapSectionsFeatures = {"input": ["DEMOD_IFLEVEL", "DEMOD_EBNOVALUE", "DEMOD_EBNOVALUEQCHL"],
        "carrierlock": ["DEMOD_CARRIERLOCK", "DEMOD_CARRIEROFFSET"],
        "bitlock": ["DEMOD_BITLOCK", "DEMOD_BITLOCKQCHL", "DEMOD_BITRATEOFFSET"]
        }
        strParts = ["input", "carrierlock", "bitlock"]
        mapBinaryFeaturesAndValidNumber = {"DEMOD_CARRIERLOCK": 2, "DPU_FRAMESYNCSTATUS1": 2, "DPU_FRAMESYNCSTATUS2": 2}
        mapTypesAndTimeDepentFeatures = {"counter": ["DPU_ERRORBITNUMBER1", "DPU_TOTALBITNUMBER1", "DPU_RSERRORBITNUMBER1", 
        "DPU_ERRORBITNUMBER2", "DPU_TOTALBITNUMBER2", "DPU_ERRORBITNUMBER2", "DPU_RSERRORBITNUMBER2", 
        "DEMOD_ERRORBITNUMBER", "DEMOD_ERRORBITNUMBERQCHL", "DEMOD_TOTALBITNUMBER", "DEMOD_TOTALBITNUMBERQCHL", 
        "DEMOD_TOTALBITNUMBERJCHL", "DEMOD_ERRORBITNUMBERJCHL", 
        "DEMOD_VITERBI1TOTALBITNUMBER", "DEMOD_VITERBI2TOTALBITNUMBER", "DEMOD_VITERBI1ERRORBITNUMBER", "DEMOD_VITERBI2ERRORBITNUMBER", 
        "DRU_TASKNUMBER1", "DRU_TASKNUMBER2"]}
        #fn_cullTimeEffectOfAJob(mapTypesAndTimeDepentFeatures, strJobDir)
        #fn_regularBoolFeaturesOfAJob(mapBinaryFeaturesAndValidNumber, strJobDir)
        #fn_extractValidRecordsOfAJob(strJobDir)
        #fn_constructSectionsOfAJob(mapSectionsFeatures, strParts, strJobDir)
        fn_generateSamplesFromAJob(strJobDir)
        """
