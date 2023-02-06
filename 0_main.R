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

x<- tasks_df[1,] %>% select(c(cdr,fe,ms1,ms2)) %>% as.list(.)
names(x)
for (role in names(x)) {
  print(role)
  print(x[[role]])
}



library(matlib)
team <- 'C5M1'
sync_matrix <- readxl::read_excel('gAndP_table2.xlsx')
sync_matrix_sq <- sync_matrix
sync_matrix_sq[,2:5] <- sync_matrix[,2:5]^2
sync_matrix$driver <- rowSums(sync_matrix_sq[,2:5])
empath_scores <- colSums(sync_matrix_sq[,2:5])
empath <- names(empath_scores[which.max(empath_scores)])
data_to_update <- rownames_to_column(as.data.frame(empath_scores), "from") %>% right_join(sync_matrix) %>%
  select(from,empath_scores,driver) %>%
  pivot_longer(!from, names_to = "sync_metric", values_to = "value")

# save empath and driver scores

# make overall Se
v_prime <- cbind(sync_matrix['from'],sync_matrix[empath])
v_prime <- v_prime[which(v_prime$from != empath),] %>% select(-from)
v_prime <- as.vector(v_prime[,empath])

M <- sync_matrix[which(sync_matrix$from != empath), -which(names(sync_matrix) == empath)] %>% select(-c(driver,from))
Q <- matlib::inv(as.matrix(M)) %*% v_prime

s_e<- v_prime%*%Q

newRow <- c(
  'from' = team,
  'sync_metric' = 's_e',
  'value' = s_e)
data_to_update %>% rbind(newRow)