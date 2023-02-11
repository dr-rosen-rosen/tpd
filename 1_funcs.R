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
  tl_df$start_time <- as.POSIXct(tl_df$start_time, format = "%m/%d/%Y %H:%M", tz = "America/Chicago") 
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
          c("MMSEV-EVA","Project RED and Survey (Multiteam Task)",
            'MMSEV', 'Project RED and Survey (FUSION)') ~ "team_action",
          "Rover" ~ "dyad_action",
          c("Morning DPC","Evening DPC", "Morning DPC (Voice or Text)", 'Morning DPC (text)') ~ "team_transition",
          .default = 'none'
        ),
        task_num = seq(from=1, to=nrow(tl_df),by=1)
    ) 
  
  tl_df[which(tl_df$cdr == 'M'),'cdr'] <- "NA"
  tl_df[which(tl_df$fe == 'M'),'fe'] <- "NA"
  tl_df[which(tl_df$ms1 == 'M'),'ms1'] <- "NA"
  tl_df[which(tl_df$ms2 == 'M'),'ms2'] <- "NA"
  
  tl_df[grepl("starts|; |NEDA|NBVP|:",tl_df$cdr),'cdr'] <- "NA"
  tl_df[grepl("starts|; |NEDA|NBVP|:",tl_df$fe),'fe'] <- "NA"
  tl_df[grepl("starts|; |NEDA|NBVP|:",tl_df$ms1),'ms1'] <- "NA"
  tl_df[grepl("starts|; |NEDA|NBVP|:",tl_df$ms2),'ms2'] <- "NA"
  
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
  role_e4_list <- r %>% select(c(cdr,fe,ms1,ms2)) %>% as.list(.)
  start_time <- r$start_time #lubridate::with_tz(r$start_time, tzone = 'America/Chicago')
  end_time <- start_time + lubridate::minutes(r$duration_min)
  
  for (role in names(role_e4_list)) {

    if (role_e4_list[[role]] != "NA") {
      t <- dplyr::tbl(con,paste0(tolower(role_e4_list[[role]]),'_',measure))
      one_e4 <- t %>%
        dplyr::filter(time_stamp >= start_time , time_stamp <= end_time) %>%
        dplyr::collect()
      print(nrow(one_e4))
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
        print('... has NO data!')
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
    #print(fromRole)
    role_acf <- e4_df[,fromRole] %>% drop_na() %>% acf(plot = FALSE, lag.max = offset)
    print('done acf')
    print(role_acf$acf[offset])
    syncCoefs[[fromRole,fromRole]] <- role_acf$acf[offset]
    print('stored acf')
    role_arima <- arima(e4_df[,fromRole], order = c(1,0,0), optim.control = list(maxit = 4000), method="ML")
    print('done arima')
    # syncCoefs[[fromRole,fromRole]] <- role_arima$coef[['ar1']]
    e4_df[,fromRole] <- residuals(role_arima)
  }
  ### Fills in rest of the Table 1 matrix using residual timeseries
  for (fromRole in workingRoles){
    toRoles <- workingRoles[workingRoles != fromRole]
    for (toRole in toRoles) {
      #print(paste('toRole:',(toRole)))
      syncCoefs[[fromRole,toRole]] <- ccf(e4_df[,toRole], e4_df[,fromRole], plot = FALSE, lag.max = offset)$acf[offset]
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
  driver_scores <- syncMatrix %>% select(driver) %>% rownames_to_column("team_or_role_id")
  data_to_update <- rownames_to_column(as.data.frame(empath_scores), "team_or_role_id") %>% 
    right_join(driver_scores, by = "team_or_role_id") %>%
    pivot_longer(!team_or_role_id, names_to = "s_metric_type", values_to = "synch_coef")
  # print(data_to_update)
  # # make overall Se
  if (nrow(syncMatrix) > 2) {
    v_prime <- as.vector(syncMatrix[which(rownames(syncMatrix) != empath),empath])
    M <- syncMatrix[which(rownames(syncMatrix) != empath), which(colnames(syncMatrix) != empath)] %>% select(-c(driver))
    Q <- matlib::inv(as.matrix(M)) %*% v_prime
    s_e<- v_prime%*%Q
    newRow <- c(
      'team_or_role_id' = 'team',
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
      print(head(e4_df))
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
