import shutil
import os

def fn_copyZipsToDstDir(strZipsSrcDir, strZipsDstDir):
        for name in os.listdir(strZipsSrcDir):
                if name.startswith("JOB"):
                        strZipFileSrc = os.path.join(strZipsSrcDir, name)
                        strZipFileDst = os.path.join(strZipsDstDir, name)
                        shutil.copyfile(strZipFileSrc, strZipFileDst)


if __name__ == "__main__":
        strZipsSrcDir = "d:\\zips"
        strZipsDstDir = ""
        #os.mkdir(strZipsDstDir)
        fn_copyZipsToDstDir(strZipsSrcDir, strZipsDstDir)