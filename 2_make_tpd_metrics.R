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
  'create_ACC_energy_metric_sfly', 'pull_e4_data', 'make_sync_matrix'
  # variables to export
  
  )
)
clusterEvalQ(cl, {
  library(magrittr)
  library(tidyverse)
  # library(dplyr)
  # library(dbplyr)
  library(DBI)
  #con <- DBI::dbConnect(RSQLite::SQLite(), 'OR_data.db')
  con <- DBI::dbConnect(RPostgres::Postgres(),
                        dbname   = 'OR_DB',
                        host     = 'localhost',
                        port     = 5432,
                        user     = 'postgres',
                        password = 'LetMeIn21')
  NULL
})
registerDoParallel(cl)
getDoParWorkers()

# dyad_izer_par_db( #cmbd_dyad_borg_par_db(#dyad_izer_par_db( #borgattizer_par_db(
#   df = fam_df
# )
parallel::stopCluster(cl)
