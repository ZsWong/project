import os
import zipfile
import shutil

import re
rePnWorkSchRep = re.compile(r"WorkSchRep")
rePnDemod = re.compile(r"Status.*?Demod.*?\.csv")

def fn_buildJobFromZip2Dir(strZipFile, strJobDir):
        with zipfile.ZipFile(strZipFile) as oZipFile:
                for name in oZipFile.namelist():
                        if rePnWorkSchRep.search(name):
                                strWorkSchRepXML = os.path.join(strJobDir, "WorkSchRep.xml")
                                with oZipFile.open(name) as f:
                                        with open(strWorkSchRepXML, "wb") as f1:
                                                f1.write(f.read())
                        elif rePnDemod.search(name):
                                strDemodFileName = re.match(r"Demod..*?\.csv", name)[0]
                                strDemodDir = os.path.join(strJobDir, strDemodFileName)
                                fn_build



def fn_buildDemodDir(strDemodFile, strJobDir):


if __name__ == "__main__":
        strZipFile = "/home/zswong/workspace/data/zips/JOB201911051541200.zip"
        strJobDir = "/home/zswong/workspace/data/jobs"
        fn_buildJobFromZip2Dir(strZipFile, strJobDir)
                                        

