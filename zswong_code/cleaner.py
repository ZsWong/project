import shutil
import os

def fn_copyZipsToDstDir(strZipsSrcDir, strZipsDstDir):
        for name in os.listdir(strZipsSrcDir):
                if name.startswith("JOB"):
                        strZipFileSrc = os.path.join(strZipsSrcDir, name)
                        strZipFileDst = os.path.join(strZipsDstDir, name)
                        shutil.copyfile(strZipFileSrc, strZipFileDst)


if __name__ == "__main__":
        strZipsSrcDir = "/home/zswong/repository/detection/data/zips"
        strZipsDstDir = "/home/zswong/workspace/data/zips"
        #os.mkdir(strZipsDstDir)
        fn_copyZipsToDstDir(strZipsSrcDir, strZipsDstDir)