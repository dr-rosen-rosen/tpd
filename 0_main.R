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

# a <- "/Users/mrosen44/Johns Hopkins University/Salar Khaleghzadegan - Project_NASA/HERA/Campaign 5/Mission 1/E4 Data for HERA C5M1/MD-1/1550323404_A01435/EDA.csv"
# df <- loadE4CsvToDB(
#   fPath = a,
#   con = DBI::dbConnect(RPostgres::Postgres(),
#                        dbname   = config$dbname, 
#                        host     = 'localhost',
#                        port     = config$dbPort,
#                        user     = config$dbUser,
#                        password = config$dbPw)
# )


# gets all mission day folders
# list.dirs(
#   '/Users/mrosen44/Johns\ Hopkins\ University/Salar\ Khaleghzadegan\ -\ Project_NASA/HERA/Campaign\ 5/Mission\ 1/E4\ Data\ for\ HERA\ C5M1',
#   recursive = FALSE
# )

# for each mission day, gets all the E4 csv files (for all badges and all variables)
mission_files <- list.files(
  c(
    # 'D:\\HERA\\Campaign 5\\Mission 1\\E4 Data for HERA C5M1',
    # 'D:\\HERA\\Campaign 5\\Mission 2\\E4 Data for HERA C5M2',
    # 'D:\\HERA\\Campaign 5\\Mission 3\\E4 Data for HERA C5M3',
    # 'D:\\HERA\\Campaign 5\\Mission 4\\E4 Data for HERA C5M4'
    # 'D:\\HERA\\Campaign 6\\Mission 1\\E4 Data for HERA C6M1',
    # 'D:\\HERA\\Campaign 6\\Mission 2\\E4 Data for HERA C6M2'
    'D:\\HERA\\Campaign 5',
    'D:\\HERA\\Campaign 6'
  ),
  recursive = TRUE,
  pattern = "*.csv$",
  full.names = TRUE) %>%
  stringr::str_subset("EDA.csv$|HR.csv$|ACC.csv$|BVP.csv$|TEMP.csv$",negate = FALSE)

con <- DBI::dbConnect(RPostgres::Postgres(),
                      dbname   = config$dbname, 
                      host     = 'localhost',
                      port     = config$dbPort,
                      user     = config$dbUser,
                      password = config$dbPw)
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



x <- read.csv("D:\\HERA\\Campaign 6\\Mission 1\\E4 Data for HERA C6M1/MD-11/1634040254_A0189A/ACC.csv", skip = 3)

tail(x)
x<-x[complete.cases(x),]

cols <- c('X6',
          'X.54',
          'X10') 
x[cols] <- lapply(x[,cols],as.integer)


