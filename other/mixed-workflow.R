
library(arrow)
library(dplyr)
library(dbplyr)
library(duckdb)
library(lubridate)

nyc_taxi_jan <- open_dataset("~/Datasets/nyc-taxi/year=2022/month=1/")

nyc_taxi_zones <- read_csv_arrow("data/taxi_zone_lookup.csv") |>
  janitor::clean_names() |>
  arrow_table(schema = schema(
    location_id = int64(),
    borough = utf8(),
    zone = utf8(),
    service_zone = utf8()
  ))

nyc_taxi_jan |>
  to_duckdb() |>
  window_order(pickup_datetime) |>
  mutate(
    trip_id = row_number(),
    weekday = wday(pickup_datetime, label = TRUE, abbr = FALSE)
  ) |>
  filter(
    weekday %in% c("Saturday", "Sunday"),
    second(pickup_datetime) == 59,
    minute(pickup_datetime) == 59
  ) |>
  left_join(
    nyc_taxi_zones |> to_duckdb(),
    c("pickup_location_id" = "location_id")
  ) |>
  filter(str_detect(zone, "Lincoln Square")) |>
  select(trip_id, pickup_datetime, weekday, zone) |>
  collect() |>
  print()


