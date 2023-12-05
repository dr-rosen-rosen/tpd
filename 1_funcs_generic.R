##############################################################################
##############################################################################
##############
##############  Functions for TPDV2
##############  This is modified to use the heartPy and neurokit2 HR and EDA measures
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
    physio_metric = character(), # e.g., bpm, ibi, etc. 
    s_metric_type = character(), # s_e, empath, driver
    synch_coef = numeric(),
    offset = integer()
  )
  
  sync_df_rw <- data.frame(
    task_num = integer(),
    team_or_part_id = character(), # team or part ID
    physio_signal = character(), # EDA, HR, ACC
    physio_metric = character(), # e.g., bpm, ibi, etc. 
    s_metric_type = character(), # s_e, empath, driver
    synch_coef = numeric(),
    offset = integer(),
    rw_len = integer(),
    time_stamp = POSIXct()
  )
  
  unobtrusive_df <- data.frame(
    task_num = integer(),
    team_or_part_id = character(),
    unob_signal = character(),
    metric = character(),
    value = numeric()
  )
  
  cardiac_ind_summary <- data.frame(
    task_num = integer(),
    study_member_id = character(),
    raw_ibi_row_cnt = integer(),
    bpm = numeric(),
    ibi = numeric(),
    sdnn = numeric(),
    sdsd = numeric(),
    rmssd = numeric(),
    pnn20 = numeric(),
    pnn50 = numeric(),
    hr_mad = numeric(),
    sd1 = numeric(),
    sd2 = numeric(),
    s = numeric(),
    'sd1//sd2' = numeric(),
    breathingrate = numeric()
  )

  eda_ind_summary <- data.frame(
    task_num = integer(),
    study_member_id = character(),
    mean_eda_clean = numeric(),
    mean_eda_tonic = numeric(),
    mean_eda_phasic = numeric(),
    scr_peak_count = numeric(),
    raw_eda_row_cnt = integer()
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
  
  if(!DBI::dbExistsTable(con,'sync_metrics_rw')) {
    DBI::dbCreateTable(con,'sync_metrics_rw',sync_df_rw)
  }
  
  if(!DBI::dbExistsTable(con,'unob_metrics')) {
    DBI::dbCreateTable(con,'unob_metrics',unobtrusive_df)
  }
  
  if(!DBI::dbExistsTable(con,'cardiac_ind_summary')) {
    DBI::dbCreateTable(con,'cardiac_ind_summary',cardiac_ind_summary)
  }
  
  if(!DBI::dbExistsTable(con,'eda_ind_summary')) {
    DBI::dbCreateTable(con,'eda_ind_summary',eda_ind_summary)
  }
  
}

##############
##############  File processing for task lists is study specific
##############


##############
##############  Synch metrics
##############

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

pull_e4_data <- function(r, metric,tbl_sufix, con) {
  df_list = list()
  all_e4_data <- TRUE
  
  # version for HERA:
  r %>% dplyr::select(c(cdr,fe,ms1,ms2)) %>% print()
  role_e4_list <- r %>% dplyr::select(c(cdr,fe,ms1,ms2)) %>% as.list(.)
  
  # version for PICU
  # t <- dplyr::tbl(con,'task_list')
  # role_e4_list <- t %>%
  #   dplyr::collect() 
  # # Gets a list of all e4 IDs in the target segment to pull
  # role_e4_list <- role_e4_list[which(role_e4_list$task_num == r$task_num),] %>%
  #   dplyr::select(e4_id) %>%
  #   unlist()
  
  start_time <- r$start_time #lubridate::with_tz(r$start_time, tzone = 'America/Chicago')
  end_time <- start_time + lubridate::minutes(r$duration_min)
  
  for (role in role_e4_list) {
    if (role != "NA") {
      t <- dplyr::tbl(con,paste0(tolower(role),tbl_sufix))
      one_e4 <- t %>%
        dplyr::filter(time_stamp >= start_time , time_stamp <= end_time) %>%
        dplyr::collect() %>%
        dplyr::select(time_stamp,!!as.symbol(metric)) ################################# NOT SURE THIS WORKS
      print(nrow(one_e4))
      if (nrow(one_e4) > 0) { # checks if anything is in the data, and adds to list if there is
        #print('... has data!')
        #print(head(one_e4))
        # sets col name to part_id; This is done to track who is who once they are integrated
        colnames(one_e4)[which(names(one_e4) == metric)] <- role
        #print(head(one_e4))
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
    #print(head(all_data))
    all_data <- all_data %>%
      mutate(time_stamp = floor_date(time_stamp, unit = "minute")) %>% 
      group_by(time_stamp) %>%
      summarise(across(everything(), ~ mean(.x, na.rm = TRUE))) %>%
      ungroup()
    #print(head(all_data))
      
    print(paste('All data this big... ',ncol(all_data),' by ',nrow(all_data)))
    print(paste('Min timestamp:',min(all_data$time_stamp),' and Max timestamp:',max(all_data$time_stamp)))
    return(all_data)
  } else {
    return('WHOOOPS')
  }
}

pull_e4_data_sfly <- purrr::possibly(.f = pull_e4_data, otherwise = NULL)

make_sync_matrix <- function(e4_df, offset){
  ### define datastructures for making and storing coef_matrix
  workingRoles <- colnames(e4_df)
  workingRoles <- workingRoles[workingRoles!="time_stamp"]
  syncCoefs <- data.frame(matrix(ncol=length(workingRoles),nrow=length(workingRoles), dimnames=list(workingRoles, workingRoles)))

  ### Creates diagonal (ARs) in Table 1 in Guastello and Perisini; and saves timeseris residuals with AR removed
  for (fromRole in workingRoles){
    role_acf <- e4_df[,fromRole] %>% stats::acf(plot = FALSE, lag.max = offset, na.action = na.contiguous)
    syncCoefs[[fromRole,fromRole]] <- role_acf$acf[offset]
    role_arima <- stats::arima(e4_df[,fromRole], order = c(1,0,0), optim.control = list(maxit = 4000), method="ML")
    e4_df[,fromRole] <- stats::residuals(role_arima)
  }

  ### Fills in rest of the Table 1 matrix using residual timeseries
  for (fromRole in workingRoles){
    toRoles <- workingRoles[workingRoles != fromRole]
    for (toRole in toRoles) {
      syncCoefs[[fromRole,toRole]] <- stats::ccf(e4_df[,toRole], e4_df[,fromRole], na.action=na.contiguous, plot = FALSE, lag.max = offset)$acf[offset]
    }
  }
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

get_synchronies <- function(task_list, physio_signal, tbl_sufix, metric, offset, con) {
  foreach::foreach(
    r = iterators::iter(task_list, by = 'row'),
    .combine = rbind, .noexport = 'con') %dopar% {
      # get E4 data
      #tbl_sufix <- tbl_sufix_dict[physio_signal]
      print(paste('Starting task num:',r$task_num))
      e4_df <- pull_e4_data_sfly(r, metric, tbl_sufix, con)
      # create tpd measures
      syncMatrix <- make_sync_matrix_sfly(e4_df, offset)
      sync_df <- get_sync_metrics_sfly(syncMatrix)
      if (nrow(sync_df > 0)) {
        sync_df <- sync_df %>%
          mutate(
            task_num = r$task_num,
            physio_signal = physio_signal,
            physio_metric = metric,
            offset = offset
          )
        DBI::dbAppendTable(con, 'sync_metrics',sync_df)
      }
      NULL
    }
}

get_synchronies_rw <- function(task_list, physio_signal, tbl_sufix_dict, metric, offset, rw_len, con) {
  foreach::foreach(
  r = iterators::iter(task_list, by = 'row'),
  .combine = rbind, .noexport = 'con', .errorhandling = 'remove', .final = function(x) NULL) %do% {

    # for (r in iterators::iter(task_list, by = 'row')) {
      # get E4 data
      #tbl_sufix <- tbl_sufix_dict[physio_signal]
      print(paste('Starting task num:',r$task_num))
      e4_df <- pull_e4_data_sfly(r, metric, tbl_sufix, con)
      
      if (!is.null(e4_df)) {
        # get matrix over each interval
        rw_matrix <- runner::runner(
          e4_df,
          k = rw_len, # window length
          f = make_sync_matrix_sfly,
          offset = offset 
        )
        
        # getting sync metrics per interval
        synch_df_rw <- lapply(rw_matrix,get_sync_metrics_sfly)
        
        # adding timestamps to each matrix, binding into one datframe
        # and adding additional info
        synch_df_rw <- mapply(function(x, y) { 
          # This adds a timestamp column to each matrix in the list
          x$time_stamp <- y 
          return(x)}, synch_df_rw, e4_df$time_stamp) %>% 
          dplyr::bind_rows() %>%
          filter(time_stamp > e4_df$time_stamp[[rw_len]]) %>% #drop starting rows without complete window data
          mutate(
            task_num = r$task_num,
            physio_signal = physio_signal,
            physio_metric = metric,
            offset = offset,
            rw_len = rw_len
          )
        
        #print(head(synch_df_rw))
        print(nrow(synch_df_rw))
        
        # save info
        if (nrow(synch_df_rw) > 0) {
          
          print(paste('Saving task:',r$task_num))
          DBI::dbAppendTable(con, 'sync_metrics_rw',synch_df_rw)
          print(paste('Done saving task:',r$task_num))
        }
      } else {print(paste('Problem with task:',r$task_num))}

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

