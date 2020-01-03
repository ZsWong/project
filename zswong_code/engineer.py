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
        strRegStatusFile = os.path.join(strStatusDir, "reg_status.csv")
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
        strPositiveSectionsDir = os.path.join(strSamplesDir, "positive/sections")
        strNegativeSectionsDir = os.path.join(strSamplesDir, "negtive/sections")
        os.makedirs(strPositiveSectionsDir)
        os.makedirs(strNegativeSectionsDir)
        """
        name is like "Demodx"
        """
        for name in os.listdir(strJobDir):
                if "Demod" in name:
                        strStatusDir = os.path.join(strJobDir, name)
                        strValStatusFile = os.path.join(strStatusDir, "val_status.csv")
                        pdDfValStatus = pd.read_csv(strValStatusFile)
                        bnpNArrFilter = (pdDfValStatus.loc[:, ["DPU_FRAMESYNCSTATUS1", "DPU_FRAMESYNCSTATUS2"]] == 1).values
                        bnpNArrFilter = np.apply_along_axis(fn_isAllTrue, 1, bnpNArrFilter)
                        fn_generateSamplesOfAStatus(bnpNArrFilter, strStatusDir, strPositiveSectionsDir, strNegativeSectionsDir)
def fn_generateSamplesOfAStatus(bnpNArrFilter, strStatusDir, strPositiveSectionsDir, strNegativeSectionsDir):
        strSectionsDir = os.path.join(strStatusDir, "sections")
        for name in os.listdir(strSectionsDir):
                strSectionDir = os.path.join(strSectionsDir, name)
                strPositiveSectionDir = os.path.join(strPositiveSectionsDir, name)
                if not os.path.exists(strPositiveSectionDir):
                        os.mkdir(strPositiveSectionDir)
                strNegativeSectionDir = os.path.join(strNegativeSectionsDir, name)
                if not os.path.exists(strNegativeSectionDir):
                        os.mkdir(strNegativeSectionDir)
                fn_generateSamplesFromASection(bnpNArrFilter, strSectionDir, strPositiveSectionDir, strNegativeSectionDir)
def fn_generateSamplesFromASection(bnpNArrFilter, strSectionDir, strPositiveSectionDir, strNegativeSectionDir):
        strSectionFile = os.path.join(strSectionDir, "status.csv")
        pdDfSectionStatus = pd.read_csv(strSectionFile)

        strPositiveSectionSamplesFile = os.path.join(strPositiveSectionDir, "samples.npy")
        npNArrPositiveSamples = pdDfSectionStatus.loc[bnpNArrFilter, :].values
        if os.path.exists(strPositiveSectionSamplesFile):
                npNArrPositiveFormerRecords = np.load(strPositiveSectionSamplesFile)
                npNArrPositiveSamples  = np.concatenate((npNArrPositiveFormerRecords, npNArrPositiveSamples), axis=0)
        np.save(strPositiveSectionSamplesFile, npNArrPositiveSamples)

        strNegativeSectionSamplesFile = os.path.join(strNegativeSectionDir, "samples.npy")
        npNArrNegativeSamples = pdDfSectionStatus.loc[~bnpNArrFilter, :].values
        if os.path.exists(strNegativeSectionSamplesFile):
                npNArrNegativeFormerRecords = np.load(strNegativeSectionSamplesFile)
                npNArrNegativeSamples  = np.concatenate((npNArrNegativeFormerRecords, npNArrNegativeSamples), axis=0)
        np.save(strNegativeSectionSamplesFile, npNArrNegativeSamples)
                
if __name__ == "__main__":
        strJobDir = "/home/zswong/workspace/station_code/jobs/JOB201912170654200"
        mapSectionsFeatures = {"input": ["DEMOD_IFLEVEL", "DEMOD_EBNOVALUE", "DEMOD_EBNOVALUEQCHL"],
        "carrierlock": ["DEMOD_CARRIERLOCK", "DEMOD_CARRIEROFFSET"],
        "bitlock": ["DEMOD_BITLOCK", "DEMOD_BITLOCKQCHL", "DEMOD_BITRATEOFFSET"]
        }
        strParts = ["input", "carrierlock", "bitlock"]
        mapBinaryFeaturesAndValidNumber = {"DEMOD_CARRIERLOCK": 2, "DPU_FRAMESYNCSTATUS1": 2, "DPU_FRAMESYNCSTATUS2": 2}
        #fn_regularBoolFeaturesOfAJob(mapBinaryFeaturesAndValidNumber, strJobDir)
        #fn_extractValidRecordsOfAJob(strJobDir)
        #fn_constructSectionsOfAJob(mapSectionsFeatures, strParts, strJobDir)
        fn_generateSamplesFromAJob(strJobDir)
