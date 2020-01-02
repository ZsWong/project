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
        strStatusFile = os.path.join(strStatusDir, "status.csv")
        strValidFile = os.path.join(strStatusDir, "valid")
        if os.path.exists(strValidFile):
                return

        pdDfStatus = pd.read_csv(strStatusFile)
        strpdSeriesFilter = pdDfStatus.loc[:, "RECTIME"]
        pdTstmpSeriesFilter = strpdSeriesFilter.apply(lambda t: pd.Timestamp(t))
        pdDfFiltered = pdDfStatus.loc[(pdTstmpSeriesFilter >= pdTstmpStart) & (pdTstmpSeriesFilter <= pdTstmpEnd), :]
        pdDfFiltered.to_csv(strStatusFile)
        with open(strValidFile, "wb") as f:
                f.write()

def fn_getValidPeriod(oETWorkSchRep):
         strReceivingStartTime = "/content/equipmentInfo/receivingStartTime"
         strReceivingEndTime = "/content/equipmentInfo/receivingEndTime"
         opdTstmpStart = pd.Timestamp(oETWorkSchRep.find(strReceivingStartTime).text)
         opdTstmpEnd = pd.Timestamp(oETWorkSchRep.find(strReceivingEndTime).text)
        return [opdTstmpStart, opdTstmpEnd]

def fn_constructSectionsOfAJob(mapNamePart, strParts, strBinaryFeatures, strJobDir):
        for name in os.listdir(strJobDir):
                if "Demod" in name:
                        strStatusDir = os.path.join(strJobDir, name)
                        strSectionsDir = os.path.join(strStatusDir, "sections")
                        if os.path.exists(strSectionsDir):
                                shutil.rmtree(strSectionsDir)
                        os.mkdir(strSectionsDir)
                        strStatusFile = os.path.join(strStatusDir, "status.csv")
                        fn_splitStatusIntoSections(mapNamePart, strParts, strBinaryFeatures, strStatusFile, strSectionsDir)


# Set a map between part names and  lists of features
# Along with a list of part names to set the ordinary of features
def fn_splitStatusIntoSections(mapNamePart, strParts, strBinaryFeatures, strStatusFile, strSectionsDir):
        pdDfStatus = pd.read_csv(strStatusFile)
        fn_regularBoolFeatures(strBinaryFeatures, pdDfStatus)

        for i in range(len(strParts)):
                pdDfStatusFiltered = fn_filterStatusOut(mapNamePart,strParts[:i + 1], pdDfStatus)
                strSectionName = "_".join(strParts[:i + 1]) 
                strSectionFile = os.path.join(strSectionsDir, strSectionName)
                os.mkdir(strSectionFile)
                strSectionStatusFile = os.path.join(strSectionFile, "status.csv")
                pdDfStatusFiltered.to_csv(strSectionStatusFile)

def fn_filterStatusOut(mapNamePart, strParts, pdDfStatus):
        strFeaturesFilter = []
        for part in strParts:
                strFeaturesFilter += mapNamePart[part]
        pdDfStatusFiltered = pdDfStatus.filter(strFeaturesFilter)
        return pdDfStatusFiltered

def fn_regularBoolFeatures(strBinaryFeatures, pdDfStatus):
        def fn_regular(x, y):
                if y > x:
                        return 1
                else:
                        return 0
        for feature in strBinaryFeatures:
                pdSeries = pdDfStatus.loc[:, feature]
                nMin = pdSeries.min()
                nMax = pdSeries.max()
                if nMax == 1:
                        continue
                pdSeries = pdSeries.apply(partial(fn_regular, nMin))
                pdDfStatus.loc[:, feature] = pdSeries

"""
It's better to contain all records.
So there is no need in getting the number of samples as
it's input parameter.
"""
"""
callable object
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
                        pdDfStatus = pd.read_csv(strStatusDir)
                        bnpNArrFilter = (pdDfStatus.loc[:, "DPU_FRAMESYNCSTATUS1", "DPU_FRAMESYNCSTATUS2"] == 1).values
                        bnpNArrFilter = np.apply_along_axis(fn_isAllTrue, 0, bnpNArrFilter)
                        fn_generateSamplesFromAStatus(bnpNArrFilter, strStatusDir, strSamplesDir)
def fn_generateSamplesFromAStatus(bnpNArrFilter, strStatusDir, strPositiveDir, strNegativeDir):
        strSectionsDir = os.path.join(strStatusDir, "sections")
        for name in os.listdir(strSectionsDir):
                strSectionDir = os.path.join(strSectionsDir, name)
                strPositiveSectionDir = os.path.join(strPositiveDir, name)
                if os.path.exists(strPositiveSectionDir):
                        os.mkdir(strPositiveSectionDir)
                strNegativeSectionDir = os.path.join(strNegativeDir, name)
                if os.path.exists(strNegativeSectionDir):
                        os.mkdir(strNegativeSectionDir)
                fn_generateSamplesFromASection(bnpNArrFilter, strSectionDir, strPositiveSectionDir, strNegativeSectionDir)
def fn_generateSamplesFromASection(bnpNArrFilter, strSectionDir, strPositiveSectionDir, strNegativeSectionDir):
        strSectionFile = os.path.join(strSectionDir, "status.csv")
        pdDfStatus = pd.read_csv(strSectionFile)

        strPositiveSectionStatusFile = os.path.join(strPositiveSectionDir, "samples.npy")
        npNArrPositive = np.load(strPositiveSectionStatusFile)
        npNArrPositiveRecords = pdDfStatus.loc[bnpNArrFilter, :].values[:, 1:]
        npNArrPositive  = np.concatenate((npNArrPositive, npNArrPositiveRecords), axis=0)
        np.save(strPositiveSectionStatusFile, npNArrPositive)

        strNegativeSectionStatusFile = os.path.join(strNegativeSectionDir, "samples.npy")
        npNArrNegative = np.load(strNegativeveSectionStatusFile)
        npNArrNegativeRecords = pdDfStatus.loc[~bnpNArrFilter, :].values[:, 1:]
        npNArrNegative = np.concatenate((npNArrNegative, npNArrNegativeRecords), axis=0)
        np.save(strNegativeSectionStatusFile, npNArrNegative)
                
if __name__ == "__main__":
        strJobDir = "/home/zswong/workspace/station_code/jobs/JOB20191218406217"
        strCsvFile = os.path.join(g_strJobDir, "Demod6/Demod6.csv")
        strSectionsDir = os.path.join(g_strJobDir, "Demod6/sections")
        fn_extractValidRecordsOfAJob(g_strJobDir)
        strParts = ["input", "carrierlock", "bitlock"]
        mapNamePart = {"input": ["DEMOD_IFLEVEL", "DEMOD_EBNOVALUE", "DEMOD_EBNOVALUEQCHL"],
        "carrierlock": ["DEMOD_CARRIERLOCK", "DEMOD_CARRIEROFFSET"],
        "bitlock": ["DEMOD_BITLOCK", "DEMOD_BITLOCKQCHL", "DEMOD_BITRATEOFFSET"],
        "framesync": ["DPU_FRAMESYNCSTATUS1", "DPU_FRAMESYNCSTATUS2"]}

        strParts = ["input", "carrierlock", "bitlock", "framesync"]
        strBinaryFeatures = ["DEMOD_CARRIERLOCK", "DEMOD_BITLOCK", "DEMOD_BITLOCKQCHL", 
        "DPU_FRAMESYNCSTATUS1", "DPU_FRAMESYNCSTATUS2"]
        fn_constructSectionsOfAJob(mapNamePart, strParts, strBinaryFeatures, strJobDir)        
        #fn_generateSamplesFromAJob(10, strJobDir)


