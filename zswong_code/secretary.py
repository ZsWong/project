from parameter import *
import os
import zipfile
import shutil

import re
rePnWorkSchRep = re.compile(r"WorkSchRep")
rePnDemod = re.compile(r"Status.*?Demod.*?\.csv")
rePnNumber = re.compile(r"[0-9]+?")

def fn_buildJobsDirFromZipsDir(strZipsDir, strJobsDir):
        for name in os.listdir(strZipsDir):
                strZipFile = os.path.join(strZipsDir, name)
                strZipName, _ = os.path.splitext(name)
                strJobDir = os.path.join(strJobsDir, strZipName)
                os.mkdir(strJobDir)
                fn_buildJobFromZip2Dir(strZipFile, strJobDir)
                
def fn_buildJobFromZip2Dir(strZipFile, strJobDir):
        with zipfile.ZipFile(strZipFile) as oZipFile:
                for name in oZipFile.namelist():
                        if rePnWorkSchRep.search(name):
                                strWorkSchRepXML = os.path.join(strJobDir, "WorkSchRep.xml")
                                with oZipFile.open(name) as f:
                                        with open(strWorkSchRepXML, "wb") as f1:
                                                f1.write(f.read())
                        elif rePnDemod.search(name):
                                strDemodFileName = re.search(r"Demod.*?\.csv", name)[0]
                                strNumber = rePnNumber.search(strDemodFileName)[0]
                                strDemodName = "Demod" + strNumber
                                strDemodDir = os.path.join(strJobDir, strDemodName)
                                os.mkdir(strDemodDir)
                                strDemodFile = os.path.join(strDemodDir,  "status.csv")
                                with oZipFile.open(name) as f:
                                        with open(strDemodFile, "wb") as f1:
                                                f1.write(f.read())



if __name__ == "__main__":
        g_strZipsDir = "/home/zswong/workspace/data/zips"
        g_strJobsDir = "/home/zswong/workspace/data/jobs"
        if os.path.exists(g_strJobsDir):
                shutil.rmtree(g_strJobsDir)
        os.mkdir(g_strJobsDir)
        fn_buildJobsDirFromZipsDir(g_strZipsDir, g_strJobsDir)
                                        

