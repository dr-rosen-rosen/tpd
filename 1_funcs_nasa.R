##############################################################################
##############################################################################
##############
##############  Functions for TPD
##############
##############################################################################
##############################################################################
library(tidyverse)


##############
##############  File processing
##############

get_task_lists <- function(data_dir) {
  file.list <- list.files(here(data_dir),full.names = TRUE)
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
          c("Dinner","Lunch","Post-Sleep Activities w/ Breakfast","Pre-Sleep Activities") ~ 'social',
          c("MMSEV-EVA","Project RED and Survey (Multiteam Task)",'Project RED Training (Multiteam Task)',
            'MMSEV', 'Project RED and Survey (FUSION)',"Crew Reflexivity (Multiteam Task)",
            'Team Task Battery and Survey (Multiteam Task)','Team Relationship Maintenance Task (Team Effectiveness',
            'Team Relationship Maintenance Short Task (Team Effectiveness)',"OBT - Loss of Pressure (EHS)",
            "OBT - Smoke Alarm (EHS)","Crew Photo Sort"
            ) ~ "team_action",
          "Rover" ~ "dyad_action",
          c("Morning DPC","Evening DPC", "Morning DPC (Voice or Text)", 'Morning DPC (text)') ~ "team_transition",
          .default = 'none'
        ),
        task_num = seq(from=1, to=nrow(tl_df),by=1)
    )

  tl_df[which(tl_df$cdr == 'M'),'cdr'] <- NA
  tl_df[which(tl_df$fe == 'M'),'fe'] <- NA
  tl_df[which(tl_df$ms1 == 'M'),'ms1'] <- NA
  tl_df[which(tl_df$ms2 == 'M'),'ms2'] <- NA

  #tl_df[grepl("starts|; |NEDA|NBVP|:",tl_df$cdr),'cdr'] <- NA
  #tl_df[grepl("starts|; |NEDA|NBVP|:",tl_df$fe),'fe'] <- NA
  #tl_df[grepl("starts|; |NEDA|NBVP|:",tl_df$ms1),'ms1'] <- NA
  #tl_df[grepl("starts|; |NEDA|NBVP|:",tl_df$ms2),'ms2'] <- NA
  
  tl_df <- tl_df %>%
    mutate(
      cdr = if_else(nchar(cdr) != 6, substr(cdr,start=1,stop=6),cdr),
      fe = if_else(nchar(fe) != 6, substr(fe,start=1,stop=6),fe),
      ms1 = if_else(nchar(ms1) != 6, substr(ms1,start=1,stop=6),ms1),
      ms2 = if_else(nchar(ms2) != 6, substr(ms2,start=1,stop=6),ms2)
    )
  
  # removes (for now) instnaces with more than one e4 per task
  tl_df[grepl("/",tl_df$cdr),'cdr'] <- NA
  tl_df[grepl("/",tl_df$fe),'fe'] <- NA
  tl_df[grepl("/",tl_df$ms1),'ms1'] <- NA
  tl_df[grepl("/",tl_df$ms2),'ms2'] <- NA
  
  tl_df$cdr[tl_df$cdr == "NA"] <- NA
  tl_df$fe[tl_df$fe == "NA"] <- NA
  tl_df$ms1[tl_df$ms1 == "NA"] <- NA
  tl_df$ms2[tl_df$ms2 == "NA"] <- NA

  return(tl_df)
}
