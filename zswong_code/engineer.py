from parameter import * 
import pandas as pd
import numpy as np

import os
import shutil
import xml.etree.ElementTree as ET
from functools import partial

def fn_extractValidRecordsOfAJob(strJobDir):
        nOrdinals = []
        for name in os.listdir(strJobDir):
                if "Demod" in name:
                        n = int(name[5:])
                        nOrdinals.append(n)
        nnpNArrOrdinal = np.array(nOrdinals)
        nnpNArrOrdinal.sort()

        strWorkSchRepXMLFile = os.path.join(strJobDir, "WorkSchRep.xml")
        oETWorkSchRep = ET.parse(strWorkSchRepXMLFile)
        for n in range(nnpNArrOrdinal.size):
                oTstmps = fn_getValidPeriod(n, oETWorkSchRep)
                strDemodName = "Demod" + str(nnpNArrOrdinal[n])
                strDemodFile = os.path.join(strJobDir, os.path.join(strDemodName, "status.csv"))
                fn_extractValidRecords(oTstmps[0], oTstmps[1], strDemodFile)

def fn_extractValidRecords(pdTstmpStart, pdTstmpEnd, strCsvFile):
        pdDf = pd.read_csv(strCsvFile)
        if not "RECTIME" in pdDf.columns:
                return
        strpdSeriesFilter = pdDf.loc[:, "RECTIME"]
        pdTstmpSeriesFilter = strpdSeriesFilter.apply(lambda t: pd.Timestamp(t))
        pdDf.loc[:, "RECTIME"] = pdTstmpSeriesFilter
        pdDfFiltered = pdDf.loc[(pdDf["RECTIME"] >= pdTstmpStart) & (pdDf["RECTIME"] <= pdTstmpEnd), :]
        pdDfFiltered.drop(columns = ["RECTIME"], inplace = True)
        pdDfFiltered.to_csv(strCsvFile)

def fn_getValidPeriod(nOrdinal, oETWorkSchRep):
        strChannelHierarchy = "/content/Result/receivingResult/channel"
        oElementChannel = oETWorkSchRep.findall(strChannelHierarchy)[nOrdinal]
        oElementReceivingTstmp = oElementChannel.findall("receivingTimes")[-1]
        opdTstmpStart = pd.Timestamp(oElementReceivingTstmp.find("receivingStartTime").text)
        opdTstmpEnd = pd.Timestamp(oElementReceivingTstmp.find("receivingEndTime").text)
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

def fn_generateSamplesFromAJob(nSamples, strJobDir):
        strSamplesDir = os.path.join(strJobDir, "samples")
        if os.path.exists(strSamplesDir):
                shutil.rmtree(strSamplesDir)
        strPositiveDir = os.path.join(strSamplesDir, "positive")
        strNegativeDir = os.path.join(strSamplesDir, "negtive")
        os.makedirs(strPositiveDir)
        os.makedirs(strNegativeDir)
        
        for name in os.listdir(strJobDir):
                if "Demod" in name:
                        strStatusDir = os.path.join(strJobDir, name)
                        if fn_isFramesync(strStatusDir):
                                fn_generateSampleFromAStatusDir(10, strStatusDir, strPositiveDir)
                        else:
                                fn_generateSampleFromAStatusDir(10, strStatusDir, strNegativeDir)

def fn_generateSampleFromAStatusDir(nSamples, strStatusDir, strDstDir):
        strDemodName = os.path.basename(strStatusDir)

        strDstSectionsDir = os.path.join(strDstDir, strDemodName + "/sections")
        os.makedirs(strDstSectionsDir)

        strSectionsDir = os.path.join(strStatusDir, "sections")
        for name in os.listdir(strSectionsDir):
                strSectionName = os.path.splitext(name)[0]
                strSectionDir = os.path.join(strSectionsDir, strSectionName)
                pdDfStatus = pd.read_csv(os.path.join(strSectionDir, "status.csv"))

                nnpIndexes = np.linspace(0, pdDfStatus.shape[0], nSamples, endpoint = False, dtype = np.int)
                npNArrStatus = pdDfStatus.iloc[nnpIndexes, :].values

                strDstSectionDir = os.path.join(strDstSectionsDir, strSectionName)
                os.mkdir(strDstSectionDir)
                strNpyFile = os.path.join(strDstSectionDir, "status.npy")
                np.save(strNpyFile, npNArrStatus)
                
def fn_isFramesync(strStatusDir):
        strStatusFile = os.path.join(strStatusDir, "status.csv")
        pdDf = pd.read_csv(strStatusFile)
        bpdSeriesFramesyncStatus1 = pdDf["DPU_FRAMESYNCSTATUS1"]
        bpdSeriesFramesyncStatus2 = pdDf["DPU_FRAMESYNCSTATUS2"]
        if len(set(bpdSeriesFramesyncStatus1)) != 1 or len(set(bpdSeriesFramesyncStatus2)) != 1:
                return False
        return True 

if __name__ == "__main__":
        strJobDir = "/home/zswong/workspace/data/jobs/JOB201912184062170"
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
        fn_generateSamplesFromAJob(10, strJobDir)


