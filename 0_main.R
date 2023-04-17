##############################################################################
##############################################################################
##############
############## Main scripts for TPD
##############
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
source('1_funcs_picu.R')
# Sys.setenv(RETICULATE_PYTHON = config$py_version)
# reticulate::source_python('1_funcs.py')

# for each mission day, gets all the E4 csv files (for all badges and all variables)
mission_files <- list.files(
  c(
    # 'D:\\HERA\\Campaign 5',
    # 'D:\\HERA\\Campaign 6'
    #'/Users/mrosen44/Johns\ Hopkins\ University/Salar\ Khaleghzadegan\ -\ Project_NASA/HERA/Campaign\ 5/Mission\ 1/E4\ Data\ for\ HERA\ C5M1/MD-1'
    'D:\\PICU'
  ),
  recursive = TRUE,
  pattern = "*.csv$",
  full.names = TRUE) %>%
  # stringr::str_subset("EDA.csv$|HR.csv$|ACC.csv$|BVP.csv$|TEMP.csv$",negate = FALSE)
  stringr::str_subset("IBI.csv$",negate = FALSE)

con <- DBI::dbConnect(RPostgres::Postgres(),
                      dbname   = 'e4_picu',#config$dbname,
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

# for (measure in c('eda','hr')) {
#   for (offset in c(5,20,60)) {
#     print(paste('STARTING:',measure,offset))
#     get_synchronies(
#       # task_list = tasks_df[which(tasks_df$task_num <= 10),],
#       # task_list = tasks_df_short[which(tasks_df_short$task_num == 1),],
#       task_list = tasks_df_short,
#       measure = measure,
#       offset = offset,
#       con = DBI::dbConnect(RPostgres::Postgres(),
#                            dbname   = config$dbname, 
#                            host     = 'localhost',
#                            port     = config$dbport,
#                            user     = config$dbUser,
#                            password = config$dbPw)
#     )
#   }
# }

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

get_mean_physio(
  task_list = tasks_df_short,
  measure = 'hr',
  con = DBI::dbConnect(RPostgres::Postgres(),
                       dbname   = config$dbname, 
                       host     = 'localhost',
                       port     = config$dbport,
                       user     = config$dbUser,
                       password = config$dbPw)
)




sync_df_all <- DBI::dbReadTable(
  conn = DBI::dbConnect(RPostgres::Postgres(),
                        dbname   = config$dbname, 
                        host     = 'localhost',
                        port     = config$dbport,
                        user     = config$dbUser,
                        password = config$dbPw),
  name = 'sync_metrics'
) %>%
  unite(variable, c(physio_signal, s_metric_type, offset_secs)) %>%
  pivot_wider(
    names_from = variable,
    values_from = synch_coef
  )


mean_physio_df_all <- DBI::dbReadTable(
  conn = DBI::dbConnect(RPostgres::Postgres(),
                        dbname   = config$dbname, 
                        host     = 'localhost',
                        port     = config$dbport,
                        user     = config$dbUser,
                        password = config$dbPw),
  name = 'unob_metrics'
) %>%
  unite(variable, c(unob_signal, metric)) %>%
  pivot_wider(
    names_from = variable,
    values_from = value
  )
team_phys <- sync_df_all[which(sync_df_all$team_or_part_id == 'team'),] %>%
  select(task_num, contains("_s_e_"))
sync_df_all <- sync_df_all %>%
  filter(team_or_part_id != 'team') %>%
  select(-contains("_s_e_"))
all_unob_df <- full_join(mean_physio_df_all,sync_df_all, by = c('task_num','team_or_part_id')) %>%
  left_join(team_phys, by = 'task_num') %>%
  rename(e4_id = team_or_part_id)


df_all <- tasks_df %>% left_join(all_unob_df, by = c('task_num','e4_id'))
write.csv(df_all,'df_all.csv')

