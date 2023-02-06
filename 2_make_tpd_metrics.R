##############################################################################
##############################################################################
##############
##############  Sets up environment to parallel process TPD metrics
##############
##############################################################################
##############################################################################



# Define any variables that need to be sent to cluster export

# spin up cluster
cl <- makeCluster(6, outfile="")
clusterExport(cl=cl, varlist = c(
  
  # functions to export
  'create_ACC_energy_metric_sfly', 'pull_e4_data_sfly', 'make_sync_matrix_sfly',
  'get_sync_metrics_sfly'
  # variables to export
  
  )
)
clusterEvalQ(cl, {
  library(magrittr)
  library(tidyverse)
  library(matlib)
  library(DBI)
  con <- DBI::dbConnect(RPostgres::Postgres(),
                        dbname   = 'e4_hera',#config$dbname, 
                        host     = 'localhost',
                        port     = 5433,#config$dbPort,
                        user     = 'script_monkey',#config$dbUser,
                        password = 'cocobolo32'#config$dbPw
                        )
  NULL
})
registerDoParallel(cl)
getDoParWorkers()

get_synchronies(
  task_list = tasks_df[which(tasks_df$task_num <= 5),],
  measure = 'hr',
  offset = 50,
  con = con)

parallel::stopCluster(cl)
