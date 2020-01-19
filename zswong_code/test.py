strJobsDir = "d:/docker_for_winter/jobs"
strZipsDir = "d:/zips"
strSpecialDir = "d:/special"

import shutil
import os

listStrSpecialZipFiles = [name for name in os.listdir(strZipsDir) if os.path.splitext(name)[0] not in os.listdir(strJobsDir)]
for zip in listStrSpecialZipFiles:
    strSrcZipFile = os.path.join(strZipsDir, zip)
    strDstZipFile = os.path.join(strSpecialDir, zip)
    shutil.copyfile(strSrcZipFile, strDstZipFile)