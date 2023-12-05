##############################################################################
##############################################################################
##############
############## Main scripts for TPD V2
##############  This is modified to use the heartPy and neurokit2 HR and EDA measures
##############
##############################################################################
##############################################################################

library(tidyverse)
library(config)
library(here)
library(janitor)
library(lubridate)
library(foreach)
library(parallel)
library(doParallel)
library(matlib)
library(here)
# library(reticulate)

config <- config::get()
source('1_funcs_generic.R')
source('1_funcs_nasa.R')
#source('1_funcs_picuV2.R')
# Sys.setenv(RETICULATE_PYTHON = config$py_version)
# reticulate::source_python('1_funcs.py')

# for each mission day, gets all the E4 csv files (for all badges and all variables)
mission_files <- list.files(
  c(
    'D:\\HERA\\Campaign 5'#,
    #'D:\\HERA\\Campaign 6'
    #'/Users/mrosen44/Johns\ Hopkins\ University/Salar\ Khaleghzadegan\ -\ Project_NASA/HERA/Campaign\ 5/Mission\ 1/E4\ Data\ for\ HERA\ C5M1/MD-1'
    # 'D:\\PICU'
  ),
  recursive = TRUE,
  pattern = "*.csv$",
  full.names = TRUE) %>%
  # stringr::str_subset("EDA.csv$|HR.csv$|ACC.csv$|BVP.csv$|TEMP.csv$",negate = FALSE)
  # stringr::str_subset("IBI.csv$",negate = FALSE)
  #stringr::str_subset("EDA.csv$|HR.csv$|ACC.csv$|BVP.csv$|TEMP.csv$|IBI.csv$",negate = FALSE)
  stringr::str_subset("EDA.csv$|BVP.csv$",negate = FALSE)

con <- DBI::dbConnect(RPostgres::Postgres(),
                      dbname   = 'e4_hera',#'e4_picu',#config$dbname,
                      host     = 'localhost',
                      port     = 5432,#config$dbPort,
                      user     = 'postgres',#'script_monkey',#config$dbUser,
                      password = 'LetMeIn21'#'cocobolo32'#config$dbPw
                      )
Sys.time()
for (f in mission_files) {
  loadE4CsvToDB(
    fP = f,
    con = con
  )
}
Sys.time()
beepr::beep()
DBI::dbDisconnect(con)

#' 
#' 
#for (t in DBI::dbListTables(con)) {
#  DBI::dbRemoveTable(con,t)
#}

tasks_df <- get_task_lists(
    data_dir = 'data/HERA_tasklists'
) %>%
  filter(!is.na(team) & task_category != 'none')

pre_db_tasks_and_metrics(
  tasks_df = tasks_df,
  con = DBI::dbConnect(RPostgres::Postgres(),
                       dbname   = 'e4_hera',#config$dbname, 
                       host     = 'localhost',
                       port     = config$dbport,
                       user     = config$dbUser,
                       password = config$dbPw),
  overwrite = FALSE
)

e4s <- unique(as.vector(as.matrix(tasks_df[c('cdr','fe','ms1','ms2')])))


con <- DBI::dbConnect(RPostgres::Postgres(),
                     dbname   = 'e4_hera',#config$dbname, 
                     host     = 'localhost',
                     port     = config$dbport,
                     user     = config$dbUser,
                     password = config$dbPw)
measure <- 'EDA'
for (e4 in e4s) {
  t <- paste0(tolower(e4),'_',tolower(measure))
  if (!DBI::dbExistsTable(con,t)) { # check if table exists in database; create if it doesn't
    print(glue::glue('Problem with: {t}'))
  }
}


tasks_df_short <- tasks_df %>%
  filter(team == "C6M4") %>%
  distinct() %>%
  arrange(task_num)


cardiac_metrics <- c('bpm', 'ibi', 'sdnn', 'sdsd', 'rmssd', 'pnn20', 'pnn50', 'hr_mad', 'sd1', 'sd2', 's','breathingrate','sd1/sd2')
#eda_metrics <- c('eda_clean', 'eda_tonic', 'eda_phasic')
eda_metrics <- c('eda_phasic')
tbl_sufix_dict <- c('eda' = '_eda_nk2', 'cardiac' = '_hpy_rolling')

offset = 2 # cardiac measures are sampled at 30 second intervals; eda at 4hz; the offset is # of positions (not seconds)
for (metric in eda_metrics) {
  print(paste('STARTING:',metric))
  get_synchronies(
    # task_list = tasks_df[which(tasks_df$task_num <= 10),],
    #task_list = tasks_df_short[which(tasks_df_short$task_num == 304),],
    task_list = tasks_df,
    physio_signal = 'eda',
    tbl_sufix_dict = tbl_sufix_dict,
    metric = metric,
    offset = offset,
    con = DBI::dbConnect(RPostgres::Postgres(),
                         dbname   = 'e4_hera',#config$dbname,
                         host     = 'localhost',
                         port     = config$dbport,
                         user     = config$dbUser,
                         password = config$dbPw)
  )
  }



##################
###### RW Se 
##################

tasks_df_short_rw <- tasks_df %>%
  filter(shift_chunk == 0) %>%
  mutate(duration_min = 720) %>%
  select(task_num, start_time, duration_min) %>%
  distinct() %>%
  arrange(task_num)

cardiac_metrics <- c('bpm', 'ibi', 'sdnn', 'sdsd', 'rmssd', 'pnn20', 'pnn50', 'hr_mad', 'sd1', 'sd2', 's','breathingrate','sd1/sd2')
eda_metrics <- c('eda_clean', 'eda_tonic', 'eda_phasic')


# cardiac_metrics <- c('rmssd')
# eda_metrics <- c('eda_clean')
tbl_sufix_dict <- c('eda' = '_eda_nk2', 'cardiac' = '_hpy_rolling')

# eda_metrics <- c('micro_s')
# tbl_sufix_dict <- c('eda' = '_eda', 'cardiac' = '_hpy_rolling')

offset = 2 # cardiac measures are sampled at 30 second intervals; eda at 4hz; the offset is # of positions (not seconds)
rw_len = 30
for (metric in cardiac_metrics) {
  print(paste('STARTING:',metric))
  get_synchronies_rw(
    # task_list = tasks_df[which(tasks_df$task_num <= 10),],
    #task_list = tasks_df_short[which(tasks_df_short$task_num == 304),],
    # task_list = tasks_df_short_rw[which(tasks_df_short_rw$task_num == 289),],
    task_list = tasks_df_short_rw,
    physio_signal = 'cardiac',
    tbl_sufix_dict = tbl_sufix_dict,
    metric = metric,
    offset = offset,
    rw_len = rw_len,
    con = DBI::dbConnect(RPostgres::Postgres(),
                         dbname   = config$dbname,
                         host     = 'localhost',
                         port     = config$dbport,
                         user     = config$dbUser,
                         password = config$dbPw)
  )
}


con <- DBI::dbConnect(RPostgres::Postgres(),
                      dbname   = config$dbname,
                      host     = 'localhost',
                      port     = config$dbport,
                      user     = config$dbUser,
                      password = config$dbPw)
t <- dplyr::tbl(con,'sync_metrics_rw')
one_e4 <- t %>%
  dplyr::filter(task_num == 265, s_metric_type == 's_e',physio_metric == 'eda_clean') %>%
  dplyr::collect() %>%
  select(time_stamp,synch_coef)

earlywarnings::generic_ews(
  timeseries = one_e4$synch_coef,
  winsize = 50, # 50% of timeseries length, default
  detrending = 'no',#gaussian',
  # bandwidth = ,
  span = 25, # degree of smoothing, default = 25
  # degree = ,
)

# get_synchronies(
#   # task_list = tasks_df[which(tasks_df$task_num <= 10),],
#   # task_list = tasks_df_short[which(tasks_df_short$task_num == 1),],
#   task_list = tasks_df_short,
#   measure = 'hr',
#   offset = 5,
#   con = DBI::dbConnect(RPostgres::Postgres(),
#                        dbname   = config$dbname, 
#                        host     = 'localhost',
#                        port     = config$dbport,
#                        user     = config$dbUser,
#                        password = config$dbPw)
# )
# beepr::beep()

### This is now done in heartPy and neurokit2 notebook
# get_mean_physio(
#   task_list = tasks_df_short,
#   measure = 'hr',
#   con = DBI::dbConnect(RPostgres::Postgres(),
#                        dbname   = config$dbname, 
#                        host     = 'localhost',
#                        port     = config$dbport,
#                        user     = config$dbUser,
#                        password = config$dbPw)
# )




sync_df_all <- DBI::dbReadTable(
  conn = DBI::dbConnect(RPostgres::Postgres(),
                        dbname   = "e4_hera",#config$dbname, 
                        host     = 'localhost',
                        port     = config$dbport,
                        user     = config$dbUser,
                        password = config$dbPw),
  name = 'sync_metrics'
) %>%
  unite(variable, c(physio_signal, physio_metric, s_metric_type, offset)) %>%
  pivot_wider(
    names_from = variable,
    values_from = synch_coef
  )

cardiac_ind_df_all <- DBI::dbReadTable(
  conn = DBI::dbConnect(RPostgres::Postgres(),
                        dbname   = "e4_hera",#config$dbname, 
                        host     = 'localhost',
                        port     = config$dbport,
                        user     = config$dbUser,
                        password = config$dbPw),
  name = 'cardiac_ind_summary'
) 

eda_ind_df_all <- DBI::dbReadTable(
  conn = DBI::dbConnect(RPostgres::Postgres(),
                        dbname   = "e4_hera",#config$dbname, 
                        host     = 'localhost',
                        port     = config$dbport,
                        user     = config$dbUser,
                        password = config$dbPw),
  name = 'eda_ind_summary'
) 

ind_df <- full_join(cardiac_ind_df_all, eda_ind_df_all, by = c('task_num','study_member_id')) %>%
  rename('team_or_part_id' = 'study_member_id')

team_phys <- sync_df_all[which(sync_df_all$team_or_part_id == 'team'),] %>%
  select(task_num, contains("_s_e_"))
sync_df_all <- sync_df_all %>%
  filter(team_or_part_id != 'team') %>%
  mutate(team_or_part_id = tolower(team_or_part_id)) %>%
  select(-contains("_s_e_"))
all_unob_df <- full_join(ind_df,sync_df_all, by = c('task_num','team_or_part_id')) %>%
  left_join(team_phys, by = 'task_num') %>%
  rename(e4_id = team_or_part_id) 

tasks_df2 <- tasks_df %>%
  select(-crew_members) %>%
  pivot_longer(cols = cdr:ms2,values_to = 'e4_id',names_to = 'role') %>% 
  mutate(e4_id = tolower(e4_id))

df_all <- tasks_df2  %>% full_join(all_unob_df, by = c('task_num','e4_id')) %>%
  relocate(task_num, e4_id)
write.csv(df_all,'HERA_df_physio_all_11-6-2023.csv')




################## 
###### Find dupes, etc.
#################

cardiac_ind_df_all[duplicated(cardiac_ind_df_all[c('task_num','study_member_id')]),]
