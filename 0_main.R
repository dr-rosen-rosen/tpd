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
source('1_funcs_picuV2.R')
# Sys.setenv(RETICULATE_PYTHON = config$py_version)
# reticulate::source_python('1_funcs.py')

# for each mission day, gets all the E4 csv files (for all badges and all variables)
#' mission_files <- list.files(
#'   c(
#'     # 'D:\\HERA\\Campaign 5',
#'     # 'D:\\HERA\\Campaign 6'
#'     #'/Users/mrosen44/Johns\ Hopkins\ University/Salar\ Khaleghzadegan\ -\ Project_NASA/HERA/Campaign\ 5/Mission\ 1/E4\ Data\ for\ HERA\ C5M1/MD-1'
#'     'D:\\PICU'
#'   ),
#'   recursive = TRUE,
#'   pattern = "*.csv$",
#'   full.names = TRUE) %>%
#'   # stringr::str_subset("EDA.csv$|HR.csv$|ACC.csv$|BVP.csv$|TEMP.csv$",negate = FALSE)
#'   # stringr::str_subset("IBI.csv$",negate = FALSE)
#'   stringr::str_subset("EDA.csv$|HR.csv$|ACC.csv$|BVP.csv$|TEMP.csv$|IBI.csv$",negate = FALSE)
#' 
#' con <- DBI::dbConnect(RPostgres::Postgres(),
#'                       dbname   = 'e4_picu',#config$dbname,
#'                       host     = 'localhost',
#'                       port     = 5432,#config$dbPort,
#'                       user     = 'postgres',#'script_monkey',#config$dbUser,
#'                       password = 'LetMeIn21'#'cocobolo32'#config$dbPw
#'                       )
#' Sys.time()
#' for (f in mission_files) {
#'   loadE4CsvToDB(
#'     fP = f,
#'     con = con
#'   )
#' }
#' Sys.time()
#' beepr::beep()
#' DBI::dbDisconnect(con)
#' 
#' 
#' for (t in DBI::dbListTables(con)) {
#'   DBI::dbRemoveTable(con,t)
#' }

tasks_df <- get_task_lists(
    data_dir = 'data',
    fname = 'PICU_Device_Assignment.xlsx'
)

pre_db_tasks_and_metrics(
  tasks_df = tasks_df,
  con = DBI::dbConnect(RPostgres::Postgres(),
                       dbname   = config$dbname, 
                       host     = 'localhost',
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
eda_metrics <- c('eda_clean', 'eda_tonic', 'eda_phasic')
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

get_synchronies(
  # task_list = tasks_df[which(tasks_df$task_num <= 10),],
  # task_list = tasks_df_short[which(tasks_df_short$task_num == 1),],
  task_list = tasks_df_short,
  measure = 'hr',
  offset = 5,
  con = DBI::dbConnect(RPostgres::Postgres(),
                       dbname   = config$dbname, 
                       host     = 'localhost',
                       port     = config$dbport,
                       user     = config$dbUser,
                       password = config$dbPw)
)
beepr::beep()

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
                        dbname   = config$dbname, 
                        host     = 'localhost',
                        port     = config$dbport,
                        user     = config$dbUser,
                        password = config$dbPw),
  name = 'cardiac_ind_summary'
) 

eda_ind_df_all <- DBI::dbReadTable(
  conn = DBI::dbConnect(RPostgres::Postgres(),
                        dbname   = config$dbname, 
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


df_all <- tasks_df %>% mutate(e4_id = tolower(e4_id)) %>% left_join(all_unob_df, by = c('task_num','e4_id')) %>%
  relocate(task_num, e4_id)
write.csv(df_all,'df_physio_all.csv')

