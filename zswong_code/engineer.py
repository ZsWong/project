import pandas as pd
import numpy as np

import os
import xml.etree.ElementTree as ET

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
                strDemodFile = os.path.join(strJobDir, os.path.join(strDemodName, strDemodName + ".csv"))
                fn_extractValidRecords(oTstmps[0], oTstmps[1], strDemodFile)



def fn_extractValidRecords(pdTstmpStart, pdTstmpEnd, strCsvFile):
        pdDf = pd.read_csv(strCsvFile)
        strpdSeriesFilter = pdDf.loc[:, "RECTIME"]
        pdTstmpSeriesFilter = strpdSeriesFilter.apply(lambda t: pd.Timestamp(t))
        bpdSeriesFilter = pdTstmpSeriesFilter.apply(lambda t: t >= pdTstmpStart and t <=pdTstmpEnd)
        pdDfFiltered = pdDf.loc[bpdSeriesFilter, :]
        pdDfFiltered.to_csv(strCsvFile)

def fn_getValidPeriod(nOrdinal, oETWorkSchRep):
        strChannelHierarchy = "/content/Result/receivingResult/channel"
        oElementChannel = oETWorkSchRep.findall(strChannelHierarchy)[nOrdinal]
        oElementReceivingTstmp = oElementChannel.findall("receivingTimes")[-1]
        opdTstmpStart = pd.Timestamp(oElementReceivingTstmp.find("receivingStartTime").text)
        opdTstmpEnd = pd.Timestamp(oElementReceivingTstmp.find("receivingEndTime").text)
        return [opdTstmpStart, opdTstmpEnd]




if __name__ == "__main__":
        strCsvFile = "/home/zswong/workspace/data/jobs/JOB201911051541200/Demod1/Demod1.csv"
        strJobDir = "/home/zswong/workspace/data/jobs/JOB201911051541200"
        fn_extractValidRecordsOfAJob(strJobDir)


