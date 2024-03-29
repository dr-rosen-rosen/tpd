library(RHRV)
library(tidyverse)

###################
######## Went with heartPy instead of this
##################


fPath <- '/Users/mrosen44/Johns Hopkins/Salar Khaleghzadegan - Project_CollectiveAllostaticLoad/PICU Data Collection/Shift_05/E4_data/1605872469_A0280D/IBI.csv'

# ibi$time_stamp <- start + lubridate::seconds(ibi$beat_time)
start <- lubridate::as_datetime(read.csv(fPath, nrows=1, skip=0,header = FALSE)[[1]], tz = 'UTC')
data <- read.csv(
  fPath, skip = 1, header = FALSE, colClasses = c('numeric','numeric'), col.names = c('beat_time','ibi')
) %>%
  mutate(
    time_stamp = start + lubridate::seconds(beat_time)
  )
data$beat_time <- data$beat_time - data[[1,'beat_time']]

# write.table(data$beat_time, here('data','hr_tmp','tmp_hr.csv'), sep=",", col.names = FALSE,row.names = FALSE)

# Intialize an empty data structure
hrv.data <- RHRV::CreateHRVData()
hrv.data <- SetVerbose(hrv.data,TRUE)
# hrv.data <- LoadBeatAscii(hrv.data, "tmp_hr.csv",
#                           RecordPath = "data/hr_tmp")
hrv.data = BuildNIHR(hrv.data)
hrv.data = FilterNIHR(hrv.data)
hrv.data = InterpolateNIHR(hrv.data, freqhr = 4)
PlotNIHR(hrv.data, main = "niHR")
hrv.data = CreateTimeAnalysis(hrv.data, size = 300,
                              interval = 7.8125)
hrv.data = CreateFreqAnalysis(hrv.data)
hrv.data = 
  CalculatePowerBand(hrv.data , indexFreqAnalysis = 1,
                     size = 300, shift = 30, type = "fourier",
                     ULFmin = 0, ULFmax = 0.03, VLFmin = 0.03, VLFmax = 0.05,
                     LFmin = 0.05, LFmax = 0.15, HFmin = 0.15, HFmax = 0.4 )
PlotPowerBand(hrv.data, indexFreqAnalysis = 1, ymax = 200, ymaxratio = 1.7)

hrv.data2 <- RHRV::CreateHRVData()
hrv.data2$Beat$Time <- data$beat_time
hrv.data2 = BuildNIHR(hrv.data2)
hrv.data2 = FilterNIHR(hrv.data2)
hrv.data2 = InterpolateNIHR(hrv.data2, freqhr = 4)
PlotNIHR(hrv.data2, main = "niHR")
hrv.data2 = CreateTimeAnalysis(hrv.data2, size = 300,
                              interval = 7.8125)


get_hrv_metrics <- function(e4_list, role, task_num) {
  print('Getting HRV metrics..')
  hrv.data <- RHRV::CreateHRVData()
  hrv.data$Beat$Time <- as.data.frame(e4_list[role])
  print('loaded time into hrv.data:')
  print(hrv.data$Time)
  hrv.data = BuildNIHR(hrv.data)
  hrv.data = FilterNIHR(hrv.data)
  hrv.data = InterpolateNIHR(hrv.data, freqhr = 4)
  new_row <- append(
    hrv.data$TimeAnalysis, 
    'team_or_part_id' = role,
    'task_num' = task_num)
  print(paste('HRV row for: ',task_num,'\n','new_row'))
  # PlotNIHR(hrv.data, main = "niHR")
  # hrv.data = CreateTimeAnalysis(hrv.data, size = 300,
  #                                interval = 7.8125)
  return(new_row)
}

get_ind_hrv <- function(task_list, measure, con) {
  foreach::foreach(
    r = iterators::iter(task_list, by = 'row'),
    .combine = rbind, .noexport = 'con') %do% {
      # get E4 data
      print(paste('Starting task num:',r$task_num))
      e4_list <- pull_e4_data(r, measure, con)
      print('back from pulling E4')
      print(names(e4_list))
      hrv_df <- data.frame(
        task_num = integer(),
        team_or_part_id = character(), # team or part ID
        size = numeric(),
        SDNN = numeric(),
        SDANN = numeric(),
        SDNNIDX = numeric(),
        pNN50 = numeric(),
        SDSD = numeric(),
        rMSSD = numeric(),
        IRRR = numeric(),
        MADRR = numeric(),
        TINN = numeric(),
        HRVi = numeric()
      )
      print(length(e4_list))
      if (length(e4_list) > 0) {
        print('starting cyclingthrough roles')
        for (role in names(e4_list)) {
          print(paste('...starting role:',role))
          print(as.vector(e4_list[role]))
          new_row <- get_hrv_metrics(e4_list, role, r$task_num)
          if (!plyr::empty(new_row)) {
            hrv_df <- bind_rows(hrv_df,new_row)
          }
        }

      } else {print(paste('No e4 data for:',r$task_num))}
      if (!plyr::empty(hrv_df)) {
        if (DBI::dbExistsTable('hrv_metrics')) {
          DBI::dbAppendTable(con, 'hrv_metrics',hrv_df)
        } else {
          DBI::dbCreateTable(con,'hrv_metrics', hrv_df)
          DBI::dbWriteTable(con,name = 'hrv_metrics', value = hrv_df, append = TRUE)
        }
      } else {print(paste('No hrv metrics for:',r$task_num))}
    } # end iterating through rows of tasklist
}

get_ind_hrv(
  task_list = tasks_df_short[which(tasks_df_short$task_num <=2),],
  measure = 'ibi',
  con = DBI::dbConnect(RPostgres::Postgres(),
                        dbname   = config$dbname, 
                        host     = 'localhost',
                        port     = config$dbport,
                        user     = config$dbUser,
                        password = config$dbPw))