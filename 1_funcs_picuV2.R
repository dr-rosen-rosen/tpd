##############################################################################
##############################################################################
##############
##############  Functions for TPDV2
##############  This is modified to use the heartPy and neurokit2 HR and EDA measures
##############
##############################################################################
##############################################################################
library(tidyverse)


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
      #start_time = with_tz(start_time, "UTC")
    ) %>%
    group_by(shift_day, shift_chunk) %>% mutate(task_num = cur_group_id()) %>% ungroup()
  
  return(chunked_df)
}