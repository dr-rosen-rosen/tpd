##############################################################################
##############################################################################
##############
##############  Functions for TPD
##############
##############################################################################
##############################################################################
library(tidyverse)

##############
##############  Database scripts
##############

loadE4CsvToDB <- function(fPath, con) {
  # Pass a file path and upload the data to the connection
  # If the file path is set up as it comes from empatica, this will work
  # E4 device numbers is the folder name (after *timestamp_*) and file name is measure
  
  valueList <- list( # used to set variable names usable in db
    'EDA' = 'micro_s', 'TEMP' = 'degrees_c', 'BVP' = 'bvp','HR' = 'avg_hr',
    'ACC' = c('x','y','z')
    )
  
  # extracts badge # and variable from filename
  vars <- str_extract(fPath, "[^_]+(?=\\.csv$)") %>% str_split(pattern = '/') %>% unlist()
  device <- vars[[1]]
  measure <- vars[[2]]

  # read in data
  ftype1 <- c('TEMP','EDA','BVP','HR','ACC') # these are all formatted similarly
  if (measure %in% ftype1) {
    # Get the starting timestamp and sampling frequency
    if (measure == 'ACC') {
      start <- lubridate::as_datetime(read.csv(fPath, nrows=1, skip=0,header = FALSE)[[1,1]])
      hz <- read.csv(fPath, nrows=1, skip=1,header = FALSE)[[1,1]]
    } else {
      start <- lubridate::as_datetime(read.csv(fPath, nrows=1, skip=0,header = FALSE)[[1]])
      hz <- read.csv(fPath, nrows=1, skip=1,header = FALSE)[[1]]
    }
    # Read in the rest of the data
    data <- read.csv(fPath,skip = 2,header = FALSE)
    # names(data)[names(data) == "V1"] <- valueList[[measure]]
    names(data) <- valueList[[measure]]
    # Create and add time_stamp index column
    interval <- as.difftime(1,units = 'secs') / hz
    end <- start + (interval * (nrow(data) - 1))
    time_stamp <- seq(
      from = start,
      by = interval,
      to = end)
    data['time_stamp'] <- time_stamp
  } else if (measure == 'IBI') {
    print('Not doing IBI yet')
    data <- data.frame()
  }

  # Push to DB
  t <- paste0(tolower(device),'_',tolower(measure))
  if (!DBI::dbExistsTable(con,t)) { # check if table exists in database; create if it doesn't
    DBI::dbCreateTable(con, t, data)
  } 
  DBI::dbWriteTable(con, name = t, value = data, append = TRUE)
  return(t)
}


pre_db_tasks_and_metrics <- function(tasks_df, con, overwrite) {
  
  sync_df <- data.frame(
    task_num = integer(),
    team_or_role_id = character(), # team or role
    physio_signal = character(), # EDA, HR, ACC
    s_metric_type = character(), # s_e, empath, driver
    synch_coef = numeric(),
    offset_secs = integer()
  )
  
  if(!DBI::dbExistsTable(con,'task_list')) {
    DBI::dbCreateTable(con,'task_list', tasks_df)
    DBI::dbWriteTable(con,name = 'task_list', value = tasks_df, append = TRUE)
  } else if (overwrite) {
    DBI::dbWriteTable(con, name = 'task_list', value = tasks_df, overwrite=TRUE)
  } else {
    DBI::dbWriteTable(con, name = 'task_list', value = tasks_df, overwrite=TRUE)
  }
  
  if(!DBI::dbExistsTable(con,'sync_metrics')) {
    DBI::dbCreateTable(con,'sync_metrics',sync_df)
  }
  
}
##############
##############  File processing
##############

get_task_lists <- function(data_dir) {
  file.list <- list.files(here(data_dir,'HERA_tasklists'),full.names = TRUE)
  tl_df <- lapply(file.list,readxl::read_excel) %>% bind_rows() %>% janitor::clean_names()
  
  
  # make timezones and deal with timestamps
  tl_df$start_time <- as.POSIXct(tl_df$start_time, format = "%m/%d/%Y %H:%M", tz = "America/New_York") 
  tl_df$start_time <- tl_df$start_time + lubridate::hours(1)
  tl_df$start_time <- lubridate::with_tz(tl_df$start_time, tzone = 'UTC') 
  
  # create duration column
  tl_df <- tl_df %>% separate(duration, c('hour','min')) %>%
    mutate(
      duration_min = (as.integer(hour)*60) + as.integer(min)
    ) %>%
    select(-c(hour,min))
  
  # parse mission days
  tl_df$md <- stringr::str_remove(tl_df$md,pattern = 'MD')
  tl_df$md <- stringr::str_trim(tl_df$md)
  tl_df$md <- as.integer(tl_df$md)
  
  # Create task column categories
  tl_df <- tl_df %>%
    mutate(
        task_category = dplyr::case_match(activity_name,
          c("Dinner","Lunch") ~ 'social',
          c("MMSEV-EVA","Project RED and Survey (Multiteam Task)") ~ "team_action",
          "Rover" ~ "dyad_action",
          c("Morning DPC","Evening DPC") ~ "team_transition",
          .default = 'none'
        ),
        task_num = seq(from=1, to=nrow(tl_df),by=1)
    ) %>%
    mutate(across(c(cdr,fe,ms1,ms2)), na_if, "M")
  
  
  return(tl_df)
}

###############################################################################################
#####################   OLD Functions for E4 syncrhony done in R
#####################     Ran in to MANY issues in R w/ sqlite
#####################     attempted to address that with small py functions
#####################     more issues with timestamps, and dataframes passing back and forth
#####################     for time reasons; abandoning this for now.
#####################     This will be helpful when we have time (and are not using sqlite anymore)
###############################################################################################
create_ACC_energy_metric <- function(one_e4) {
  dimensions = c('x', 'y', 'z')
  print('here')
  for (dimension in dimensions) {
    one_e4[[dimension]] <- one_e4[[dimension]]^2
  }
  one_e4$energy <- rowSums(one_e4[,dimensions])
  one_e4$energy <- '^' (one_e4$energy, 1/2)
  one_e4 <- one_e4[ , !(names(one_e4) %in% dimensions)]
  print(colnames(one_e4))
  return(one_e4)
}

create_ACC_energy_metric_sfly <- purrr::possibly(.f = create_ACC_energy_metric, otherwise = NULL)

pull_e4_data <- function(e4_ids, shift_start, measure,e4_to_part_id) {
  print(e4_to_part_id)
  shift_stop <- shift_start + lubridate::hours(12)
  print(shift_start)
  print(shift_stop)
  df_list = list()
  all_e4_data <- TRUE
  for (e4 in e4_ids) {
    ### pulls data for given badge within shift range
    
    t_name <- paste0('Table_',e4,'_',measure)
    ### built this as temporary work around to issues directly filtering timestamp data in sqlite from R
    one_e4 <- pull_e4_data_py(
      # db is hard coded in script for now, since this is temporary fix.
      t_name = t_name,
      shift_start = shift_start,
      shift_stop = shift_stop
    )
    ### This SHOULD be easy to do in R, but dbplyr does not play well with sqlite for timestamps
    ### Using python script w/sqlalchemy as temporary fix until we migrat to better db solution
    # one_e4 <- tbl(e4_con, t_name) %>%
    #   #filter(TimeStamp >= shift_start & TimeStamp <= shift_stop) %>%
    #   collect()
    # one_e4 <- one_e4 %>%
    #   filter(TimeStamp >= shift_start & TimeStamp <= shift_stop)
    
    ### does minimal data QC... this needs exapnding
    if (nrow(one_e4) > 0) { # checks if anything is in the data, and adds to list if there is
      print('... has data!')
      print(nrow(one_e4))
      ### Need to add metric conversion for ACC (go from 3 cols to 'energy metric')
      # sets col name to part_id; This is done to track who is who once they are integrated
      # need to expand this to other measures with named list of measure / metrics
      if (measure == 'HR'){
        colnames(one_e4)[which(names(one_e4) == "AvgHR")] <- e4_to_part_id[[e4]]
      } else if (measure == 'EDA') {
        colnames(one_e4)[which(names(one_e4) == "MicroS")] <- e4_to_part_id[[e4]]
      } else if (measure == 'ACC') {
        one_e4 <- create_ACC_energy_metric_r(one_e4)
        colnames(one_e4)[which(names(one_e4) == "energy")] <- e4_to_part_id[[e4]]
      }
      df_list[[e4]] <- one_e4
    } else {
      print('... has NO data!')
      all_e4_data <- FALSE
    }
  }
  if (all_e4_data == TRUE) {
    all_data <- df_list %>% purrr::reduce(full_join, by = "TimeStamp")
    all_data$TimeStamp <- lubridate::as_datetime(all_data$TimeStamp)
    all_data$TimeStamp <- lubridate::force_tz(all_data$TimeStamp, "America/New_York") # timestamps were coming back with Batlimore Times, but marked as UTC timezone; this fixes
    print(paste('All data this big... ',ncol(all_data),' by ',nrow(all_data)))
    return(all_data)
  } else {
    return('WHOOOPS')
  }
}
pull_e4_data_sfly <- purrr::possibly(.f = pull_e4_data, otherwise = NULL)

make_sync_matrix <- function(e4_data){
  ### define datastructures for making and storing coef_matrix
  print(typeof(e4_data))
  working_roles <- colnames(e4_data)
  print(working_roles)
  print(typeof(working_roles))
  working_roles <- working_roles %>% purrr::list_modify("TimeStamp" = NULL)
  print(working_roles)
  Sync_Coefs <- data.frame(matrix(ncol=length(working_roles),nrow=length(working_roles), dimnames=list(working_roles, working_roles)))
  ### format and clean timeseries
  time_lag <- 50
  ### Creates Table 1 in Guastello and Perisini
  for (from_role in working_roles){
    print(from_role)
    role_acf <- e4_data %>% select(from_role) %>% drop_na() %>% acf(plot = FALSE)
    Sync_Coefs[[from_role,from_role]] <- role_acf$acf[time_lag]
  }
  return(Sync_Coefs)
}

make_sync_matrix_sfly <- purrr::possibly(.f = make_sync_matrix, otherwise = NULL)

get_synchronies <- function(task_list, measure, offset) {
  foreach::foreach(
    r = iterators::iter(task_list, by = 'row'),
    .combine = rbind, .noexport = 'con') %dopar% {
      # get E4 data
      # pull_e4_data_sfly
      
      # create tpd measures
      # make_sync_matrix
      
      # test if there's valid data
      # write to DB... would need to iterate something like the below; 
        # write_stmt <- paste(
        #   paste0("UPDATE team_comp_metrics",borg_table_suffix),
        #   "SET team_size =",paste0(t_size,','),
        #   paste0("zeta_",shared_work_experience_window_weeks," = ",zeta,','),
        #   paste0("zeta_prime_",shared_work_experience_window_weeks," = ",zeta_prime),
        #   "WHERE LOG_ID =",paste0(LOG_ID,';'))
        # wrt <- dbSendQuery(con,write_stmt)
        # dbClearResult(wrt)
      
      NULL
    }
}

  
  e4_ids <- unique(shift_df$e4_id)
  shift_date <- unique(shift_df$date)[1] # should add a check to make sure they are all the same
  lubridate::tz(shift_date) <- "America/New_york" # changing to EDT; E4 data is in ETC... need to check that daylight savings is handled correctly
  am_or_pm <- unique(shift_df$am_or_pm)[1] # should add a check to make sure they are all the same
  if (am_or_pm == 'am') {
    lubridate::hour(shift_date) <- 7
  } else if (am_or_pm == 'pm') {
    lubridate::hour(shift_date) <- 19
  }
  #measure <- "HR" # need to pass this in as an argument
  e4_data <- pull_e4_data(
    e4_ids = e4_ids,
    shift_start = shift_date,
    measure = measure,
    e4_to_part_id = split(shift_df$study_member_id, shift_df$e4_id)
  )
  print(nrow(e4_data))
  
  # make_sync_matrix()
  # make_sync_metrics()
  # how to return results in tibble?
  return(e4_data)
}
