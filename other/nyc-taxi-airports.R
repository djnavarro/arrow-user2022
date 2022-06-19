library(arrow)
library(dplyr)

zones <- read_csv_arrow("data/taxi_zone_lookup.csv")
airport_zones <- zones %>%
  filter(stringr::str_detect(Zone, "Airport")) %>%
  pull(LocationID)

nyc_taxi <- open_dataset("~/Datasets/nyc-taxi/")

# nyc_taxi %>%
#   filter(year == 2021) %>%
#   mutate(
#     wdaytmp = wday(pickup_datetime),
#     weekday = case_when(
#       is.na(wdaytmp) ~ NA_character_,
#       wdaytmp == 1 ~ "Sunday",
#       wdaytmp == 2 ~ "Monday",
#       wdaytmp == 3 ~ "Tuesday",
#       wdaytmp == 4 ~ "Wednesday",
#       wdaytmp == 5 ~ "Thursday",
#       wdaytmp == 6 ~ "Friday",
#       wdaytmp == 7 ~ "Saturday"
#     )
#   ) %>%
#   write_dataset(
#     path = "~/Datasets/nyc-taxi-2021",
#     partitioning = c("month", "weekday"),
#     min_rows_per_group = 10000L,
#     max_open_files = 3600L
#   )


# small data set where the pickup locations are
# in airports (total size is 119MB, each month is about 10MB)
nyc_taxi %>%
  filter(
    year %in% 2019,
    pickup_location_id %in% airport_zones
  ) %>%
  write_dataset(
    path = "~/Datasets/nyc-taxi-airports",
    partitioning = c("month"),
    min_rows_per_group = 10000L,
    max_open_files = 3600L
  )



