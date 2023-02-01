##############################################################################
##############################################################################
##############
##############
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
config <- config::get()
source('1_funcs.R')

a <- "/Users/mrosen44/Johns Hopkins University/Salar Khaleghzadegan - Project_NASA/HERA/Campaign 5/Mission 1/E4 Data for HERA C5M1/MD-1/1550323404_A01435/EDA.csv"
df <- loadE4CsvToDB(
  fPath = a,
  con = DBI::dbConnect(RPostgres::Postgres(),
                       dbname   = config$dbname, 
                       host     = 'localhost',
                       port     = 5433,
                       user     = config$dbUser,
                       password = config$dbPw)
)


# gets all mission day folders
list.dirs(
  '/Users/mrosen44/Johns\ Hopkins\ University/Salar\ Khaleghzadegan\ -\ Project_NASA/HERA/Campaign\ 5/Mission\ 1/E4\ Data\ for\ HERA\ C5M1',
  recursive = FALSE
)

# for each mission day, gets all the E4 csv files (for all badges and all variables)
list.files(
  '/Users/mrosen44/Johns\ Hopkins\ University/Salar\ Khaleghzadegan\ -\ Project_NASA/HERA/Campaign\ 5/Mission\ 1/E4\ Data\ for\ HERA\ C5M1/MD-1',
  recursive = TRUE,
  pattern = ".csv",
  full.names = TRUE)


tasks_df <- get_task_lists(
    data_dir = 'data'
) 

  

pre_db_tasks_and_metrics(
  tasks_df = tasks_df,
  con = DBI::dbConnect(RPostgres::Postgres(),
                       dbname   = config$dbname, 
                       host     = 'localhost',
                       port     = 5433,
                       user     = config$dbUser,
                       password = config$dbPw)
)

