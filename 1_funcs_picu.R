##############################################################################
##############################################################################
##############
##############  Functions for TPD
##############
##############################################################################
##############################################################################
library(tidyverse)
library(matlib)

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
      start <- lubridate::as_datetime(read.csv(fPath, nrows=1, skip=0,header = FALSE)[[1,1]], tz = 'UTC')
      hz <- read.csv(fPath, nrows=1, skip=1,header = FALSE)[[1,1]]
    } else {
      start <- lubridate::as_datetime(read.csv(fPath, nrows=1, skip=0,header = FALSE)[[1]], tz = 'UTC')
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
    start <- lubridate::as_datetime(read.csv(fPath, nrows=1, skip=0,header = FALSE)[[1]], tz = 'UTC')
    data <- read.csv(
      fPath, skip = 1, header = FALSE, colClasses = c('numeric','numeric'), col.names = c('beat_time','ibi')
    ) %>%
      mutate(
        time_stamp = start + lubridate::seconds(beat_time)
      )
  }
  
  # Push to DB
  t <- paste0(tolower(device),'_',tolower(measure))
  print(fPath)
  print(paste('Exists:',exists('data')))
  print(paste('Empty:',plyr::empty(data)))
  if (!plyr::empty(data)) {
    if (!DBI::dbExistsTable(con,t)) { # check if table exists in database; create if it doesn't
      DBI::dbCreateTable(con, t, data)
    } 
    DBI::dbWriteTable(con, name = t, value = data, append = TRUE)
  } else {
    print(paste('Problems with:',fPath,'; appears empty'))
  }
  return(t)
}

pre_db_tasks_and_metrics <- function(tasks_df, con, overwrite) {
  
  sync_df <- data.frame(
    task_num = integer(),
    team_or_part_id = character(), # team or part ID
    physio_signal = character(), # EDA, HR, ACC
    s_metric_type = character(), # s_e, empath, driver
    synch_coef = numeric(),
    offset_secs = integer()
  )
  
  unobtrusive_df <- data.frame(
    task_num = integer(),
    team_or_part_id = character(),
    unob_signal = character(),
    metric = character(),
    value = numeric()
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
  
  if(!DBI::dbExistsTable(con,'unob_metrics')) {
    DBI::dbCreateTable(con,'unob_metrics',unobtrusive_df)
  }
  
}

##############
##############  File processing
##############

get_task_lists <- function(data_dir, fname) {
  tl_df <- readxl::read_excel(here(data_dir,fname)) %>% janitor::clean_names()
  
  # set overall start_time
  tl_df <- tl_df %>%
    mutate(
      start_time = if_else(
        am_or_pm == 'am', update(date, hour = 7), update(date, hour = 19)
      )
    )
  # create segments within the shift
  # chunk_list <- list()
  chunked_df <- data.frame()
  for (shift_chunk in c(0,1,2)) {
    hrs <- shift_chunk*4
    chunk_df <- tl_df %>%
      mutate(
        shift_chunk = shift_chunk,
        start_time = start_time + hours(hrs)
      )
    chunked_df <- rbind(chunked_df,chunk_df)

  }

  chunked_df <- chunked_df %>%
    select(-c(rtls_in_db,rtls_final_export,soc_bdg_loaded, soc_bdg_final_export,e4_in_db,e4_exported)) %>%
    mutate(
      duration_min = 4*60, # 4 hour chunks
      start_time = force_tz(start_time, "America/New_York"),
      start_time = with_tz(start_time, "UTC")
    ) %>%
    group_by(shift_day, shift_chunk) %>% mutate(task_num = cur_group_id()) %>% ungroup()
  
  return(chunked_df)
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
  # consider 10.1371/journal.pone.0160644
  # convert x, y , z to energy metric
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

pull_e4_data <- function(r, measure,con) {
  df_list = list()
  all_e4_data <- TRUE
  # version for HERA:
  #role_e4_list <- r %>% select(c(cdr,fe,ms1,ms2)) %>% as.list(.)
  # version for PICU
  t <- dplyr::tbl(con,'task_list')
  role_e4_list <- t %>%
    #dplyr::filter(task_num == r$task_num) %>%
    dplyr::collect() #%>%
  #print(head(role_e4_list))
  role_e4_list <- role_e4_list[which(role_e4_list$task_num == r$task_num),] %>%
    select(e4_id) %>%
    unlist()
  role_e4_list <- unname(role_e4_list)
  #role_e4_list <- unlist(role_e4_list$e4_id)
  #print(role_e4_list)
  start_time <- r$start_time #lubridate::with_tz(r$start_time, tzone = 'America/Chicago')
  end_time <- start_time + lubridate::minutes(r$duration_min)
  
  for (role in role_e4_list) {
    
    if (role != "NA") {
      t <- dplyr::tbl(con,paste0(tolower(role),'_',measure))
      one_e4 <- t %>%
        dplyr::filter(time_stamp >= start_time , time_stamp <= end_time) %>%
        dplyr::collect()
      #print(nrow(one_e4))
      if (nrow(one_e4) > 0) { # checks if anything is in the data, and adds to list if there is
        print('... has data!')
        ### Need to add metric conversion for ACC (go from 3 cols to 'energy metric')
        # sets col name to part_id; This is done to track who is who once they are integrated
        # need to expand this to other measures with named list of measure / metrics
        if (measure == 'hr'){
          colnames(one_e4)[which(names(one_e4) == "avg_hr")] <- role
        } else if (measure == 'eda') {
          colnames(one_e4)[which(names(one_e4) == "micro_s")] <- role
        } else if (measure == 'acc') {
          one_e4 <- create_ACC_energy_metric_sfly(one_e4)
          colnames(one_e4)[which(names(one_e4) == "energy")] <- role
        } 
        df_list[[role]] <- one_e4
      } else {
        #print('... has NO data!')
        all_e4_data <- FALSE
      }
    } # end of !is.na(role)
  } # end of for role in roles loop
  if (all_e4_data == TRUE) {
    all_data <- df_list %>% purrr::reduce(full_join, by = "time_stamp")
    print(paste('All data this big... ',ncol(all_data),' by ',nrow(all_data)))
    return(all_data)
  } else {
    return('WHOOOPS')
  }
}

pull_e4_data_sfly <- purrr::possibly(.f = pull_e4_data, otherwise = NULL)

make_sync_matrix <- function(e4_df, offset, measure){
  ### define datastructures for making and storing coef_matrix
  #print('Make sync matrix')
  #print(head(e4_df))
  workingRoles <- colnames(e4_df)
  workingRoles <- workingRoles[workingRoles!="time_stamp"]
  syncCoefs <- data.frame(matrix(ncol=length(workingRoles),nrow=length(workingRoles), dimnames=list(workingRoles, workingRoles)))
  #print(workingRoles)
  ### format and clean timeseries
  
  # resample to one second; HR is already at one second.
  if (measure != 'hr') {
    print('upsampling...')
    e4_df <- e4_df %>%
      mutate(time_stamp = floor_date(time_stamp, unit = "seconds")) %>% 
      group_by(time_stamp) %>%
      summarise(across(everything(), .fns = mean))
  }
  
  ### Creates diagonal (ARs) in Table 1 in Guastello and Perisini; and saves timeseris residuals with AR removed
  for (fromRole in workingRoles){
    print(fromRole)
    role_acf <- e4_df[,fromRole] %>% drop_na() %>% acf(plot = FALSE, lag.max = offset)
    #print('done acf')
    print(role_acf$acf[offset])
    syncCoefs[[fromRole,fromRole]] <- role_acf$acf[offset]
    #print('stored acf')
    role_arima <- arima(e4_df[,fromRole], order = c(1,0,0), optim.control = list(maxit = 4000), method="ML")
    #print('done arima')
    # syncCoefs[[fromRole,fromRole]] <- role_arima$coef[['ar1']]
    e4_df[,fromRole] <- residuals(role_arima)
    print(paste('done: ',fromRole))
  }
  print('done ARs')
  ### Fills in rest of the Table 1 matrix using residual timeseries
  for (fromRole in workingRoles){
    print(fromRole)
    toRoles <- workingRoles[workingRoles != fromRole]
    print(toRoles)
    for (toRole in toRoles) {
      #print(paste('toRole:',(toRole)))
      #print(syncCoefs)
      #print(ccf(e4_df[,toRole], e4_df[,fromRole], na.action=na.omit, plot = FALSE, lag.max = offset)$acf[offset])
      syncCoefs[[fromRole,toRole]] <- ccf(e4_df[,toRole], e4_df[,fromRole], na.action=na.omit, plot = FALSE, lag.max = offset)$acf[offset]
    }
  }
  print('done rest of matrix')
  return(syncCoefs)
}

make_sync_matrix_sfly <- purrr::possibly(.f = make_sync_matrix, otherwise = NULL)

get_sync_metrics <- function(syncMatrix) {
  
  syncMatrix_sq <- syncMatrix^2
  syncMatrix$driver <- rowSums(syncMatrix_sq)
  empath_scores <- colSums(syncMatrix_sq)
  empath <- names(empath_scores[which.max(empath_scores)])
  # # save empath and driver scores
  driver_scores <- syncMatrix %>% select(driver) %>% rownames_to_column("team_or_part_id")
  data_to_update <- rownames_to_column(as.data.frame(empath_scores), "team_or_part_id") %>% 
    right_join(driver_scores, by = "team_or_part_id") %>%
    pivot_longer(!team_or_part_id, names_to = "s_metric_type", values_to = "synch_coef")
  # print(data_to_update)
  # # make overall Se
  if (nrow(syncMatrix) > 2) {
    v_prime <- as.vector(syncMatrix[which(rownames(syncMatrix) != empath),empath])
    M <- syncMatrix[which(rownames(syncMatrix) != empath), which(colnames(syncMatrix) != empath)] %>% select(-c(driver))
    Q <- matlib::inv(as.matrix(M)) %*% v_prime
    s_e<- v_prime%*%Q
    newRow <- c(
      'team_or_part_id' = 'team',
      's_metric_type' = 's_e',
      'synch_coef' = s_e)
    data_to_update <- data_to_update %>% rbind(newRow)
  }
  return(data_to_update)
}

get_sync_metrics_sfly <- purrr::possibly(.f = get_sync_metrics, otherwise = NULL)

get_synchronies <- function(task_list, measure, offset, con) {
  foreach::foreach(
    r = iterators::iter(task_list, by = 'row'),
    .combine = rbind, .noexport = 'con') %do% {
      # get E4 data
      print(paste('Starting task num:',r$task_num))
      e4_df <- pull_e4_data_sfly(r, measure, con)
      # create tpd measures
      #print(head(e4_df))
      syncMatrix <- make_sync_matrix_sfly(e4_df, offset, measure)
      sync_df <- get_sync_metrics_sfly(syncMatrix)
      sync_df <- sync_df %>%
        mutate(
          task_num = r$task_num,
          physio_signal = measure,
          offset_secs = offset
        )
      if (nrow(sync_df > 0)) {
        DBI::dbAppendTable(con, 'sync_metrics',sync_df)
      }
      NULL
    }
}


#######

get_mean_physio <- function(task_list, measure, con) {
  foreach::foreach(
    r = iterators::iter(task_list, by = 'row'),
    .combine = rbind, .noexport = 'con') %do% {
      # get E4 data
      print(paste('Starting task num:',r$task_num))
      e4_df <- pull_e4_data_sfly(r, measure, con)
      if (nrow(e4_df > 0)) {
        e4_df <- e4_df %>%
          select(-time_stamp) %>%
          summarize_all(list(mean = mean,sd = sd), na.rm = TRUE) %>%
          pivot_longer(
            cols = everything(), 
            names_to = c('team_or_part_id','metric'),
            names_sep = '_',
            values_to = 'value') %>%
          mutate(
            task_num = r$task_num,
            unob_signal = measure
          )
        print(e4_df)
        DBI::dbAppendTable(con, 'unob_metrics',e4_df)
      }
      NULL
    }
}


# head(test)
# offset <- 50
# test1 <- acf(test[,'ms2'],plot = FALSE,lag.max = 50)
# test1$acf[offset]
# 
# test2 <- arima(test$cdr, order = c(1,0,0), method="ML")
# test2$coef[['ar1']]
# residuals(test2)
# 
# test3 <- arima(test$fe, order = c(1,0,0), method = 'ML')
# 
# ccf(test$cdr,test$fe,lag.max = 50, plot = FALSE)$acf[50]
# ccf(test$cdr,residuals(test2),lag.max = 50, plot = FALSE)$acf[50]
# x <- ccf(residuals(test3),residuals(test2),lag.max = 50, plot = FALSE)
# x$acf[50]
# 
# ccf(residuals(test3),residuals(test2),lag.max = 50)
# ar(test$fe)
