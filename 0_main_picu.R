##############################################################################
##############################################################################
##############
############## Main scripts for TPD V2
##############  This is modified to use the heartPy and neurokit2 HR and EDA measures
##############
##############################################################################
##############################################################################

library(tidyverse)
library(here)
library(janitor)
library(lubridate)
library(foreach)
library(parallel)
library(doParallel)
library(here)
Sys.setenv(R_CONFIG_ACTIVE = "picu")
config <- config::get()
source('1_funcs_picuV2.R')
source('1_funcs_generic.R')

tasks_df <- get_task_lists(
    data_dir = 'data',
    fname = 'PICU_Device_Assignment.xlsx'
)

pre_db_tasks_and_metrics(
  tasks_df = tasks_df,
  con = DBI::dbConnect(RPostgres::Postgres(),
                       dbname   = config$dbname, 
                       host     = config$host, #'localhost',
                       port     = config$dbport,
                       user     = config$dbUser,
                       password = config$dbPw),
  overwrite = FALSE
)


tasks_df_short <- tasks_df %>%
  select(task_num, start_time, duration_min) %>%
  distinct() %>%
  arrange(task_num)




cardiac_metrics <- c('bpm', 'ibi', 'sdnn', 'sdsd', 'rmssd', 'pnn20', 'pnn50', 'hr_mad', 'sd1', 'sd2', 's','breathingrate','sd1/sd2')
eda_metrics <- c('eda_phasic') #c('eda_clean', 'eda_tonic', 'eda_phasic')
tbl_sufix_dict <- c('eda' = '_eda_nk2', 'cardiac' = '_hpy_rolling')

offset = 2 # cardiac measures are sampled at 30 second intervals; eda at 4hz; the offset is # of positions (not seconds)
for (metric in eda_metrics) {
  print(paste('STARTING:',metric))
  get_synchronies(
    # task_list = tasks_df[which(tasks_df$task_num <= 10),],
    #task_list = tasks_df_short[which(tasks_df_short$task_num == 304),],
    task_list = tasks_df_short,
    physio_signal = 'eda',
    tbl_sufix_dict = tbl_sufix_dict,
    metric = metric,
    offset = offset,
    con = DBI::dbConnect(RPostgres::Postgres(),
                         dbname   = config$dbname,
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
                            dbname   = config$dbname,
                            host     = config$host,
                            port     = config$dbPort,
                            user     = config$dbUser,
                            password = config$dbPW
  ),
  name = 'sync_metrics'
) %>%
  unite(variable, c(physio_signal, physio_metric, s_metric_type, offset)) %>%
  pivot_wider(
    names_from = variable,
    values_from = synch_coef
  )

skimr::skim(sync_df_all)

cardiac_ind_df_all <- DBI::dbReadTable(
  conn = DBI::dbConnect(RPostgres::Postgres(),
                        dbname   = config$dbname,
                        host     = config$host,
                        port     = config$dbPort,
                        user     = config$dbUser,
                        password = config$dbPW
  ),
  name = 'cardiac_ind_summary'
) 
skimr::skim(cardiac_ind_df_all)

eda_ind_df_all <- DBI::dbReadTable(
  conn = DBI::dbConnect(RPostgres::Postgres(),
                        dbname   = config$dbname,
                        host     = config$host,
                        port     = config$dbPort,
                        user     = config$dbUser,
                        password = config$dbPW
  ),
  name = 'eda_ind_summary'
) 

skimr::skim(eda_ind_df_all)

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

skimr::skim(all_unob_df)


df_all <- tasks_df %>% mutate(e4_id = tolower(e4_id)) %>% left_join(all_unob_df, by = c('task_num','e4_id')) %>%
  relocate(task_num, e4_id)
write.csv(df_all,'PICU_df_physio_all_06-07-2024.csv')

