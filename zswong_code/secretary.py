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
import shutil

import numpy as np

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

"""
Input the type of samples needed to collect.
"""
def fn_collectSamplesFromJobs(strSamplesType, strSectionName, strJobsDir):
        npNArrs = []
        for name in os.listdir(strJobsDir):
                strJobDir = os.path.join(strJobsDir, name)
                npNArrs.append(fn_collectSamplesFromAJob(strSamplesType, strSectionName, strJobDir))
        npNArrSamples = np.concatenate(npNArrs)
        print(npNArrSamples.shape)
        return npNArrSamples
def fn_collectSamplesFromAJob(strSamplesType, strSectionName, strJobDir):
        strSamplesDir = os.path.join(strJobDir, "samples")
        strPositiveOrNegativeSamplesDir = os.path.join(strSamplesDir, strSamplesType)
        strSectionSamplesDir = os.path.join(strPositiveOrNegativeSamplesDir, "sections/" + strSectionName)
        strSamplesFile = os.path.join(strSectionSamplesDir, "samples.npy")
        npNArrSamples = np.load(strSamplesFile)
        print(npNArrSamples.shape)
        return npNArrSamples


if __name__ == "__main__":
        strZipsDir = "/home/zswong/workspace/data/zips"
        strJobsDir = "/home/zswong/workspace/station_code/jobs"

