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
        pdDfValidStatus = pd.read_csv(strValidStatusFile)
        pdDfSectionStatus = pdDfValidStatus[listStrFeatures]
        strSectionStatusFile = os.path.join(strSectionDir, "status.csv")
        pdDfSectionStatus.to_csv(strSectionStatusFile, index = False)

def fn_regularSectionOfJob(mapBinaryFeaturesAndValidNumber, strPart, strSection, strJobDir):
        listStrSectionNames = [name for name in os.listdir(strJobDir) if "Demod" in name]
        for name in listStrSectionNames:
                strStatusDir = os.path.join(strJobDir, name)
                fn_regularSectionOfStatus(mapBinaryFeaturesAndValidNumber, strPart, strSection, strStatusDir)
def fn_regularSectionOfStatus(mapBinaryFeaturesAndValidNumber, strPart, strSection, strStatusDir):
        strSectionDir = os.path.join(strStatusDir, "parts/" + os.path.join(strPart, strSection))
        strRegularizedSectionDir = os.path.join(strSectionDir, "regularized")
        if os.path.exists(strRegularizedSectionDir):
                shutil.rmtree(strRegularizedSectionDir)
        os.mkdir(strRegularizedSectionDir)

        strSectionStatusFile = os.path.join(strSectionDir, "status.csv")
        pdDfValidSectionStatus = pd.read_csv(strSectionStatusFile)
        for feature, number in mapBinaryFeaturesAndValidNumber.items():
                pdSeries = pdDfValidSectionStatus.loc[:, feature]
                pdSeriesResult = pd.Series(np.zeros(pdSeries.size))
                pdSeriesResult[pdSeries == number] = 1
                pdDfValidSectionStatus.loc[:, feature] = pdSeriesResult
        strRegularizedSectionStatusFile = os.path.join(strRegularizedSectionDir, "status.csv")
        pdDfValidSectionStatus.to_csv(strRegularizedSectionStatusFile, index = False)

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

"""
Below is a function defined to be used in np.apply_along_axis().
If elements in a row of numpy darray are all true, then return 
true for this row.
"""
def fn_isAllTrue(bnpNArr):
        for  b in bnpNArr:
                if not b:
                        return False
        return True
"""
Generate samples of a sectino of a job.
After this function, the structure of directory is like below:
part/
        section/
                samples/
                        normal/
                                test/
                                        samples.csv
                                train/
                                        samples.csv
                        abnormal/
                                sample.csv
"""
def fn_generateSamplesFromSectionOfJob(strPart, strSection, strJobDir):
        listStrStatusNames = [name for name in os.listdir(strJobDir) if "Demod" in name]
        for name in listStrStatusNames:
                strStatusDir = os.path.join(strJobDir, name)
                bNpNArrNormalIndexes, bNpNArrAbnormalIndexes= fn_getNormalIndexes(strPart, strStatusDir)
                fn_generateSamplesFromSectionOfStatus(bNpNArrNormalIndexes, bNpNArrAbnormalIndexes, 
                strPart, strSection, strStatusDir)
"""
Get the indexes of normal records according to the flag features given
by "part".
Note that a part's samples should only be sampled from normal samples of 
apprentice parts.
"""
def fn_getNormalIndexes(strPart, strStatusDir):
        strValidStatusFile = os.path.join(strStatusDir, "valid/status.csv")
        pdDfValidStatus = pd.read_csv(strValidStatusFile)
        bNpNArrFilter = (pdDfValidStatus.loc[:, ["DPU_FRAMESYNCSTATUS1", "DPU_FRAMESYNCSTATUS2"]] == 2).values
        bNpNArrFilter = np.apply_along_axis(fn_isAllTrue, 1, bNpNArrFilter)
        if strPart == "framelock":
                return bNpNArrFilter, ~bNpNArrFilter
def fn_generateSamplesFromSectionOfStatus(bNpNArrNormalIndexes, bNpNArrAbnormalIndexes, strPart, strSection, strStatusDir):
        strSamplesDir = os.path.join(strStatusDir, "parts/" + os.path.join(strPart, strSection + "/samples"))
        if os.path.exists(strSamplesDir):
                shutil.rmtree(strSamplesDir)
        os.mkdir(strSamplesDir)

        strSectionDir = os.path.join(strStatusDir, "parts/" + os.path.join(strPart, strSection))
        if strPart == "framelock":
                if strSection == "input":
                        strSectionStatusFile = os.path.join(strSectionDir, "status.csv")
                        pdDfSectionStatus = pd.read_csv(strSectionStatusFile)


        pdDfAbnormalSamples = pdDfSectionStatus.loc[bNpNArrAbnormalIndexes, :]
        strAbnormalSamplesDir = os.path.join(strSamplesDir, "abnormal")
        os.mkdir(strAbnormalSamplesDir)
        strAbnormalSamplesFile = os.path.join(strAbnormalSamplesDir, "samples.csv")
        pdDfAbnormalSamples.to_csv(strAbnormalSamplesFile, index = False)

        pdDfNormalSamples = pdDfSectionStatus.loc[bNpNArrNormalIndexes, :]
        pdDfNormalTestSamples, pdDfNormalTrainSamples = fn_splitTestAndTrain(pdDfNormalSamples, 0.20)
        strNormalSamplesDir = os.path.join(strSamplesDir, "normal")
        os.mkdir(strNormalSamplesDir)
        strNormalTestSamplesDir = os.path.join(strNormalSamplesDir, "test")
        os.mkdir(strNormalTestSamplesDir)
        strNormalTestSamplesFile = os.path.join(strNormalTestSamplesDir, "samples.csv")
        pdDfNormalTestSamples.to_csv(strNormalTestSamplesFile, index = False)
        strNormalTrainSamplesDir = os.path.join(strNormalSamplesDir, "train")
        os.mkdir(strNormalTrainSamplesDir)
        strNormalTrainSamplesFile = os.path.join(strNormalTrainSamplesDir, "samples.csv")
        pdDfNormalTrainSamples.to_csv(strNormalTrainSamplesFile, index = False)
"""
Split normal samples into test and train. 
In order to generate same samples every time, I use 
a np.random.seed(42) before generate shuffling indexes.
"""
def fn_splitTestAndTrain(pdDfTotalSamples, fPercent):
        nSamples = pdDfTotalSamples.shape[0]
        nTestSamples = int(nSamples * fPercent)
        np.random.seed(42)
        npNArrIndexes = np.random.permutation(nSamples)
        return pdDfTotalSamples.loc[npNArrIndexes[:nTestSamples], :], pdDfTotalSamples.loc[npNArrIndexes[nTestSamples:], :]

if __name__ == "__main__":
        strJobsDir = "d:/docker_for_winter/jobs"
        
        for name in os.listdir(strJobsDir):
                strJobDir = os.path.join(strJobsDir, name)
                fn_generateSamplesFromSectionOfJob("framelock", "input", strJobDir)
        """
        for name in os.listdir(strJobsDir):
                strJobDir = os.path.join(strJobsDir, name)
                fn_regularSectionOfJob({"DEMOD_CARRIERLOCK": 2}, "framelock", "carrierlock", strJobDir)
        """
        """
        for name in os.listdir(strJobsDir):
                strJobDir = os.path.join(strJobsDir, name)
                fn_constructSectionOfJob(["DEMOD_CARRIERLOCK", "DEMOD_CARRIEROFFSET"], 
        "framelock", "carrierlock", strJobDir)
        """
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
