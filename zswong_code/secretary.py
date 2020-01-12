"""
        The duty of this module is to build initial content of jobs directory from zips directory.
        Without dealing with any professional work, it just build directory and extract files needed.
        If running it as a script, it will delete former jobs directory.
        After the running of it, the jobs directory will contain files like below:
        JOBXXXXXXXXXXXXXXX
        -------Demodx
        ------------status.csv
        -------WorkSch_TASK.xml
        The status.csv file contains many invalid records in demod
"""
import os
import zipfile
import json
import shutil

import numpy as np
import pandas as pd

import re
rePnWorkSchRep = re.compile(r"WorkSch_TASK")
rePnDemod = re.compile(r"Status.*?Demod.*?\.csv")
rePnNumber = re.compile(r"[0-9]+?")

# Input the directories containing zips and jobs
# We assume "jobs directory" means the directory holding job's diretories. And 
# that is also true for "zips directory"
# You must make sure jobs directory is existing , so it can hold job directories
# If some zip files already have  corresponding job directory, skip them.
def fn_buildJobsDirFromZipsDir(strZipsDir, strJobsDir):
        if os.path.exists(strJobsDir):
                shutil.rmtree(strJobsDir)
        os.mkdir(strJobsDir)
        for name in os.listdir(strZipsDir):
                strZipFile = os.path.join(strZipsDir, name)
                strZipName, _ = os.path.splitext(name)
                strJobDir = os.path.join(strJobsDir, strZipName)
                if os.path.exists(strJobDir):
                        continue
                os.mkdir(strJobDir)
                fn_buildJobFromZip2Dir(strZipFile, strJobDir)
                
'''
 Input the full path of  a job's zip file, and the full path of job directory
 1: we tranverse the name list of the zip file 
 2: if the name of the item is a name of a work schedule file then extract it 
 3: if the name of the item is a name of a demod status file then extract it
 I found that the WorkSch_TASK file sometimes repeats in a job zip file. But it
 seems the counterparts always contain same content except for the creating time
 So it's OK that latter-found WorkSch_TASK file will cover the ealier-found
 I use corresponding pattern regularization to detect the name of item 
 '''
def fn_buildJobFromZip2Dir(strZipFile, strJobDir):
        with zipfile.ZipFile(strZipFile) as oZipFile:
                for name in oZipFile.namelist():
                        if rePnWorkSchRep.search(name):
                                strWorkSchRepXML = os.path.join(strJobDir, "WorkSch_TASK.xml")
                                with oZipFile.open(name) as f:
                                        with open(strWorkSchRepXML, "wb") as f1:
                                                f1.write(f.read())
                        elif rePnDemod.search(name):
                                strDemodFileName = re.search(r"Demod.*?\.csv", name)[0]
                                strNumber = rePnNumber.search(strDemodFileName)[0]
                                strDemodName = "Demod" + strNumber
                                strDemodDir = os.path.join(strJobDir, strDemodName)
                                os.mkdir(strDemodDir)
                                strDemodFile = os.path.join(strDemodDir,  "raw_status.csv")
                                with oZipFile.open(name) as f:
                                        with open(strDemodFile, "wb") as f1:
                                                f1.write(f.read())

def fn_collectSamplesFromJobs(strJobsDir, strJobsSamplesDir):
        if os.path.exists(strJobsSamplesDir):
                shutil.rmtree(strJobsSamplesDir)
        strJobsSectionsSamplesDir = os.path.join(strJobsSamplesDir, "sections")
        os.makedirs(strJobsSectionsSamplesDir)
        strFirstJobDir = os.path.join(strJobsDir, os.listdir(strJobsDir)[0])
        strSectionsNames = os.listdir(os.path.join(strFirstJobDir, "samples/sections"))
        fn_collectSamplesOfSections(strSectionsNames, strJobsDir, strJobsSectionsSamplesDir)
def fn_collectSamplesOfSections(strSectionsNames, strJobsDir, strJobsSectionsSamplesDir):
        for section in strSectionsNames:
                strJobsSectionSamplesDir = os.path.join(strJobsSectionsSamplesDir, section)
                os.mkdir(strJobsSectionSamplesDir)
                pdDfSamplesList = fn_collectSectionSamplesFromJobs(section, strJobsDir)
                pdDfSamples = pd.concat(pdDfSamplesList)
                strJobsSectionSamplesFile = os.path.join(strJobsSectionSamplesDir, "samples.csv")
                pdDfSamples.to_csv(strJobsSectionSamplesFile, index = False)
def fn_collectSectionSamplesFromJobs(strSection, strJobsDir):
        pdDfSamplesList = []
        for name in os.listdir(strJobsDir):
                strJobDir = os.path.join(strJobsDir, name)
                strSamplesFile = os.path.join(strJobDir, "samples/sections/" + strSection + "/samples.csv")
                pdDfSamples = pd.read_csv(strSamplesFile)
                pdDfSamplesList.append(pdDfSamples)
        return pdDfSamplesList


def fn_outputLeftFeatures(strJsonFile, strDataBaseFile, strOutputFile):
        with open(strJsonFile, "r") as f:
                mapPartsAndFeatures = json.load(f)
        pdSeriesAll = pd.read_csv(strDataBaseFile, squeeze=True, header=None) # header = None to make sure don't make "RECTIME" as the name of series
        strListedFeatures = []
        fn_extractPartsFromMap(mapPartsAndFeatures, strListedFeatures)
        strLeftedFeatures = [strFeature for strFeature in pdSeriesAll.to_list() if strFeature not in strListedFeatures]
        pdSeriesLeftedFeatures = pd.Series(strLeftedFeatures)
        pdSeriesLeftedFeatures.to_csv(strOutputFile, index = False)
def fn_extractPartsFromMap(mapPartsAndFeatures, strListedFeatures):
        for _, item in mapPartsAndFeatures.items():
                if isinstance(item, dict):
                        fn_extractPartsFromMap(item, strListedFeatures)
                else:
                        assert(isinstance(item, list))
                        strListedFeatures.extend(item)

if __name__ == "__main__":
        strZipsDir = "/home/zswong/workspace/data/zips"
        strJobsDir = "/home/zswong/workspace/station_code/jobs"

        fn_outputLeftFeatures("features.json", "features.csv", "left_features.csv")