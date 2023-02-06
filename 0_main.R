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
    '/Users/mrosen44/Johns\ Hopkins\ University/Salar\ Khaleghzadegan\ -\ Project_NASA/HERA/Campaign\ 5/Mission\ 1/E4\ Data\ for\ HERA\ C5M1/MD-1'
  ),
  recursive = TRUE,
  pattern = "*.csv$",
  full.names = TRUE) %>%
  stringr::str_subset("EDA.csv$|HR.csv$|ACC.csv$|BVP.csv$|TEMP.csv$",negate = FALSE)

con <- DBI::dbConnect(RPostgres::Postgres(),
                      dbname   = 'e4_hera',#config$dbname, 
                      host     = 'localhost',
                      port     = 5433,#config$dbPort,
                      user     = 'script_monkey',#config$dbUser,
                      password = 'cocobolo32'#config$dbPw
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
) 

pre_db_tasks_and_metrics(
  tasks_df = tasks_df,
  con = DBI::dbConnect(RPostgres::Postgres(),
                       dbname   = config$dbname, 
                       host     = 'localhost',
                       port     = config$dbport,
                       user     = config$dbUser,
                       password = config$dbPw)
)
