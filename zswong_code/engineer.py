from parameter import * 
import pandas as pd
import numpy as np

import os
import shutil
import xml.etree.ElementTree as ET
from functools import partial

def fn_extractValidRecordsOfAJob(strJobDir):
        strWorkSchRepXMLFile = os.path.join(strJobDir, "WorkSch_TASK.xml")
        oETWorkSchRep = ET.parse(strWorkSchRepXMLFile)
        pdTstmps = fn_getValidPeriod(oETWorkSchRep)

        for name in os.listdir(strJobDir):
                if "Demod" in name:
                        strStatusDir = os.path.join(strJobDir, name)
                        fn_extractValidRecords(pdTstmps[0], pdTstmps[1], strStatusDir)
def fn_extractValidRecords(pdTstmpStart, pdTstmpEnd, strStatusDir):
        strRegStatusFile = os.path.join(strStatusDir, "culled_status.csv")
        pdDfRegStatus = pd.read_csv(strRegStatusFile)
        strpdSeriesFilter = pdDfRegStatus.loc[:, "RECTIME"]
        pdTstmpSeriesFilter = strpdSeriesFilter.apply(lambda t: pd.Timestamp(t))
        pdDfFiltered = pdDfRegStatus.loc[(pdTstmpSeriesFilter >= pdTstmpStart) & (pdTstmpSeriesFilter <= pdTstmpEnd), :]
        strValStatusFile = os.path.join(strStatusDir, "val_status.csv")
        pdDfFiltered.to_csv(strValStatusFile, index = False)
def fn_getValidPeriod(oETWorkSchRep):
         strReceivingStartTime = "/content/equipmentInfo/receivingStartTime"
         strReceivingEndTime = "/content/equipmentInfo/receivingEndTime"
         opdTstmpStart = pd.Timestamp(oETWorkSchRep.find(strReceivingStartTime).text)
         opdTstmpEnd = pd.Timestamp(oETWorkSchRep.find(strReceivingEndTime).text)
         return opdTstmpStart, opdTstmpEnd

def fn_constructSectionsOfAJob(mapNamePart, strParts, strJobDir):
        for name in os.listdir(strJobDir):
                if "Demod" in name:
                        strStatusDir = os.path.join(strJobDir, name)
                        strSectionsDir = os.path.join(strStatusDir, "sections")
                        if os.path.exists(strSectionsDir):
                                shutil.rmtree(strSectionsDir)
                        os.mkdir(strSectionsDir)
                        fn_constructSectionsOfAStatus(mapNamePart, strParts, strStatusDir, strSectionsDir)
def fn_constructSectionsOfAStatus(mapNamePart, strParts, strStatusDir, strSectionsDir):
        strValStatusFile = os.path.join(strStatusDir, "val_status.csv")
        pdDfValStatus = pd.read_csv(strValStatusFile)
        for i in range(len(strParts)):
                pdDfStatusFiltered = fn_filterStatus(mapNamePart,strParts[:i + 1], pdDfValStatus)
                strSectionName = "_".join(strParts[:i + 1]) 
                strSectionFile = os.path.join(strSectionsDir, strSectionName)
                os.mkdir(strSectionFile)
                strSectionStatusFile = os.path.join(strSectionFile, "status.csv")
                pdDfStatusFiltered.to_csv(strSectionStatusFile, index = False)
def fn_filterStatus(mapNamePart, strParts, pdDfStatus):
        strFeaturesFilter = []
        for part in strParts:
                strFeaturesFilter += mapNamePart[part]
        pdDfStatusFiltered = pdDfStatus.filter(strFeaturesFilter)
        return pdDfStatusFiltered

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
        strJobDir = "/home/zswong/workspace/station_code/jobs/JOB201912170654200"
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
