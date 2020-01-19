"""
        The duty of this module is to build initial content of jobs directory from zips directory.
        Without dealing with any professional work, it just builds directory and extract files needed.
        If running it as a script, it will delete former jobs directory.
        After the running of it, the jobs directory will contain files and directories showed as below:
        WorkSch_TASK.xml
        Demod/
                status/
                        raw/
                                status.csv
                        valid/
                                status.csv  # The records in valid receiving time interval
"""
import os
import zipfile
import xml.etree.ElementTree as ElementTree
import json
import shutil

import numpy as np
import pandas as pd

import re
rePnWorkSch = re.compile(r"WorkSch_TASK.*?\.xml")
rePnDemod = re.compile(r"Status.*?Demod.*?\.csv")
rePnNumber = re.compile(r"[0-9]+?")

"""
Input the directories containing zips and jobs respectively.
We assume "jobs directory" means the directory holding job's diretories. And 
that is the case of "zips directory"
Every time we build jobs directory from zips directory, there are two options:
1. Build all jobs from zips directory
2. Build jobs that are not built yet
But we have to make sure that zips directory is existing before calling this function.
"""
def fn_buildJobsDirFromZipsDir(strZipsDir, strJobsDir, bAll = True):
        listStrJobNames = [strZipFile[:-4] for strZipFile in os.listdir(strZipsDir)]
        if bAll:
                if os.path.exists(strJobsDir):
                        shutil.rmtree(strJobsDir)
                os.mkdir(strJobsDir)
                for strJobName in listStrJobNames:
                        fn_buildJobDirFromZipsDir(strJobsDir, strJobName, strZipsDir)
        else:
                if not os.path.exists(strJobsDir):
                        os.mkdir(strJobsDir)
                for strJobName in listStrJobNames:
                        strJobDir = os.path.join(strJobsDir, strJobName)
                        if os.path.exists(strJobDir):
                                continue
                        fn_buildJobDirFromZipsDir(strJobsDir, strJobName, strZipsDir)
"""
Extract work schedule xml file and demoder status file of the zip
file given the of name of the zip file. And put them into the jobs
directory.
The directory of that job being extracted have to be non-exsiting. 
And that will be the case because we will have made jobs directory
empty before.
"""
def fn_buildJobDirFromZipsDir(strJobsDir, strJobName, strZipsDir):
        strJobDir = os.path.join(strJobsDir, strJobName)
        os.mkdir(strJobDir)
        strZipFile = os.path.join(strZipsDir, strJobName + ".zip")

        pdTimestampCreatedWorkSch = pd.Timestamp("1996-6-1")
        with zipfile.ZipFile(strZipFile) as oZipFile:
                for name in oZipFile.namelist():
                        if rePnWorkSch.search(name):
                        # WorkSch_TASK file, the schedule made earlier by station.
                        # There may be multiple WorkSch_TASK***.xml files in this zip directory.
                        # We select the newest file in this directory, according to the "createdTime".
                                strWorkSchXML = os.path.join(strJobDir, "WorkSch_TASK.xml")
                                with oZipFile.open(name) as f:
                                        oElementRoot = ElementTree.parse(f)
                                        pdTimestampCreatedWorkSchNew = pd.Timestamp(
                                                oElementRoot.find("./fileHeader/createdTime").text)
                                        if pdTimestampCreatedWorkSchNew > pdTimestampCreatedWorkSch:
                                                # Find a newer created WorkSch_TASK file
                                                # And replace older one with newer
                                                pdTimestampCreatedWorkSch = pdTimestampCreatedWorkSchNew
                                                oElementRoot.write(strWorkSchXML)
                        elif rePnDemod.search(name):
                                # Extract the name of the demod
                                strDemodFileName = re.search(r"[a-zA-Z0-9]*?\.csv", name)[0]
                                strDemodDir = os.path.join(strJobDir, os.path.splitext(strDemodFileName)[0])
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
        strZipsDir = "d:\\zips"
        strJobsDir = "d:\\docker_for_winter\\jobs"
        fn_buildJobsDirFromZipsDir(strZipsDir, strJobsDir)
        """
        for name in os.listdir(strZipsDir):
                strZipFile = os.path.join(strZipsDir, name)
                bHasDemod = False
                with zipfile.ZipFile(strZipFile) as oZipFile:
                        for name in oZipFile.namelist():
                                if rePnDemod.search(name):
                                        bHasDemod = True
                                        break
                if not bHasDemod:
                        os.remove(strZipFile)
        """
