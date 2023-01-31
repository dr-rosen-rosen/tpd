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
  print(fPath)
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
  
  # deal with rare case of malformed or NA data
  # if (measure == 'ACC') {
  #   data[,valueList[[measure]]] <- as.integer(data[,valueList[[measure]]])
  # } else {
  #   data[,valueList[[measure]]] <- as.numeric(data[,valueList[[measure]]])
  # }
  data <- data[complete.cases(data),]

  # Push to DB
  t <- paste0(tolower(device),'_',tolower(measure))
  print(paste(t,": ",nrow(data)," rows of data"))
  if (!DBI::dbExistsTable(con,t)) { # check if table exists in database; create if it doesn't
    DBI::dbCreateTable(con, t, data)
  } 
  DBI::dbWriteTable(con, name = t, value = data, append = TRUE)
  return(t)
}