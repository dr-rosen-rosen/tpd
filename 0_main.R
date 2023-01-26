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
config <- config::get()

source('1_funcs.R')

a <- "/Users/mrosen44/Johns Hopkins University/Salar Khaleghzadegan - Project_NASA/HERA/Campaign 5/Mission 1/E4 Data for HERA C5M1/MD-1/1550323404_A01435/EDA.csv"
df <- loadE4CsvToDB(
  fPath = a,
  con = DBI::dbConnect(RPostgres::Postgres(),
                       dbname   = config$dbname, #'e4_hera',
                       host     = 'localhost',
                       port     = 5433,
                       user     = config$dbUser,#'script_monkey',
                       password = config$dbPw)#'cocobolo32'
)


con <- 
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


