##############################################################################
##############################################################################
##############
##############  Sets up environment to parallel process TPD metrics
##############
##############################################################################
##############################################################################



# Define any variables that need to be sent to cluster export
tbl_sufix_dict <- c('eda' = '_eda_nk2', 'cardiac' = '_hpy_rolling')
# spin up cluster
cl <- makeCluster(20, outfile="")
clusterExport(cl=cl, varlist = c(
  
  # functions to export
  'create_ACC_energy_metric_sfly', 'pull_e4_data_sfly', 'make_sync_matrix_sfly',
  'get_sync_metrics_sfly',
  # variables to export
  'config','tbl_sufix_dict','tasks_df_short'
  )
)
clusterEvalQ(cl, {
  library(magrittr)
  library(tidyverse)
  library(lubridate)
  library(stats)
  library(DBI)
  library(purrr)
  con <- DBI::dbConnect(RPostgres::Postgres(),
                        dbname   = config$dbname,
                        host     = config$host,
                        port     = config$dbPort,
                        user     = config$dbUser,
                        password = config$dbPW
  )
  NULL
})
registerDoParallel(cl)
getDoParWorkers()

# get_synchronies(
#   task_list = tasks_df[which(tasks_df$task_num <= 5),],
#   measure = 'hr',
#   offset = 50,
#   con = con)

# tbl_sufix_dict <- c('eda' = '_eda_nk2', 'cardiac' = '_hpy_rolling')

cardiac_metrics <- c('bpm', 'ibi', 'sdnn', 'sdsd', 'rmssd','pnn20', 'pnn50', 'hr_mad', 'sd1', 'sd2', 's','breathingrate','sd1/sd2')
eda_metrics <- c('eda_clean', 'eda_tonic', 'eda_phasic')

get_synchronies(
    # task_list = tasks_df[which(tasks_df$task_num <= 10),],
    #task_list = tasks_df_short[which(tasks_df_short$task_num == 304),],
    task_list = tasks_df_short,
    physio_signal = 'eda', #'cardiac', 
    tbl_sufix = tbl_sufix_dict[['eda']], # tbl_sufix_dict[['cardiac']], 
    metric = eda_metrics[[3]], # cardiac_metrics[[6]], , 
    offset = 2, # in sync_metrics_v2 this is seconds for EDA.. CHECK!
    con = con
  )

parallel::stopCluster(cl)
beepr::beep()
