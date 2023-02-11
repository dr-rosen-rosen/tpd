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
source('1_funcs.R')
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
  stringr::str_subset("EDA.csv$|HR.csv$|ACC.csv$|BVP.csv$|TEMP.csv$",negate = FALSE)

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


for (t in DBI::dbListTables(con)) {
  DBI::dbRemoveTable(con,t)
}

tasks_df <- get_task_lists(
    data_dir = 'data'
) %>% filter(task_category != 'none')

pre_db_tasks_and_metrics(
  tasks_df = tasks_df,
  con = DBI::dbConnect(RPostgres::Postgres(),
                       dbname   = config$dbname, 
                       host     = 'localhost',
                       port     = config$dbport,
                       user     = config$dbUser,
                       password = config$dbPw)
)


get_synchronies(
  # task_list = tasks_df[which(tasks_df$task_num <= 10),],
  task_list = tasks_df,
  measure = 'eda',
  offset = 5,
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
)

link_info <- tasks_df %>% select(team,md,task_category, task_num)
sync_df_all <- sync_df_all %>% left_join(link_info, by = 'task_num')
write.csv(sync_df_all,'sync_df_all.csv')

