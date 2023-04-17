library(RHRV)
library(tidyverse)

# Intialize an empty data structure
hrv.data <- RHRV::CreateHRVData()
hrv.data <- SetVerbose(hrv.data,TRUE)


fPath <- '/Users/mrosen44/Johns Hopkins/Salar Khaleghzadegan - Project_CollectiveAllostaticLoad/PICU Data Collection/Shift_05/E4_data/1605872469_A0280D/IBI.csv'

# ibi$time_stamp <- start + lubridate::seconds(ibi$beat_time)


