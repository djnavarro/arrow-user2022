library(arrow)
library(dplyr)

# copy_files(s3_bucket("nyc-tlc"), "./all_taxi/")


# schema validation
base_dir <- "./all_taxi/trip data/yellow"
out_dir <- "./processed_new_taxi/"
files <- list.files(base_dir)

# # Some files have extra commas, remove these so that we can process them all
# # might need ot adjust the zero-length backup elsewhere.
# sed -i '' -e 's/,,,/,,/g' yellow_tripdata_2010-02.csv
# sed -i '' -e 's/,,,/,,/g' yellow_tripdata_2010-03.csv
# sed -i '' -e 's/,,//g' yellow_tripdata_2016-07.csv
# sed -i '' -e 's/,,//g' yellow_tripdata_2016-08.csv
# sed -i '' -e 's/,,//g' yellow_tripdata_2016-09.csv
# sed -i '' -e 's/,,//g' yellow_tripdata_2016-10.csv
# sed -i '' -e 's/,,//g' yellow_tripdata_2016-11.csv
# sed -i '' -e 's/,,//g' yellow_tripdata_2016-12.csv


# # for schema comparison
# output <- files |>
#   purrr::set_names() |>
#   purrr::map(function(file) {
#     message(file)
#     tryCatch(
#       {
#         tab <- read_csv_arrow(file.path(base_dir, file), as_data_frame = FALSE)
#         on.exit(rm(tab), add = TRUE)
#         tab$schema
#       },
#       error = function(e) {
#         "failed"
#       }
#
#     )
#   })


renamer <- function(nm) {
  # some columns start with spaces
  nm <- trimws(nm)

  renames <- c(
    vendor_id = "vendor_name",
    VendorID = "vendor_name",
    Trip_Pickup_DateTime = "pickup_datetime",
    tpep_pickup_datetime = "pickup_datetime",
    Trip_Dropoff_DateTime = "dropoff_datetime",
    tpep_dropoff_datetime =  "dropoff_datetime",
    Passenger_Count = "passenger_count",
    Trip_Distance = "trip_distance",
    Start_Lon = "pickup_longitude",
    Start_Lat = "pickup_latitude",
    Rate_Code = "rate_code",
    RateCodeID = "rate_code",
    RatecodeID = "rate_code",
    store_and_fwd_flag = "store_and_fwd",
    store_and_forward = "store_and_fwd",
    End_Lon = "dropoff_longitude",
    End_Lat = "dropoff_latitude",
    Payment_Type = "payment_type",
    Fare_Amt = "fare_amount",
    surcharge = "extra",
    mta_tax = "mta_tax",
    Tip_Amt = "tip_amount",
    Tolls_Amt = "tolls_amount",
    PULocationID = "pickup_location_id",
    DOLocationID = "dropoff_location_id",
    Total_Amt = "total_amount"
  )
  nm[which(nm %in% names(renames))] <- renames[nm[which(nm %in% names(renames))]]

  nm
}

proto_schema <- schema(
  vendor_name = utf8(),
  pickup_datetime = timestamp(unit = "s"),
  dropoff_datetime = timestamp(unit = "s"),
  passenger_count = int64(),
  trip_distance = float64(),
  pickup_longitude = float64(),
  pickup_latitude = float64(),
  rate_code = utf8(),
  store_and_fwd = utf8(),
  dropoff_longitude = float64(),
  dropoff_latitude = float64(),
  payment_type = utf8(),
  fare_amount = float64(),
  extra = float64(),
  mta_tax = float64(),
  tip_amount = float64(),
  tolls_amount = float64(),
  total_amount = float64(),
  improvement_surcharge = float64(),
  congestion_surcharge = float64(),
  pickup_location_id = int64(),
  dropoff_location_id = int64(),
)

confirm_it <- function(tab, col, values) {
  out <- tab %>%
    summarise(bork = all(is.na(!!!col) | !!!col %in% values)) %>%
    collect()

  if (any(out$bork)) {
    stop("There's an issue with ", col, "!")
  }

  invisible(TRUE)
}

for (file in files) {
  message("Working on ", file)

  year_month <- gsub("yellow_tripdata_", "", file)
  year_month <- gsub(".csv", "", year_month)
  year_month <- strsplit(year_month, "-")
  file_year <- as.integer(year_month[[1]][[1]])
  file_month <- as.integer(year_month[[1]][[2]])

  tab <- read_csv_arrow(file.path(base_dir, file), as_data_frame = FALSE)

  tab <- rename_with(tab, renamer) %>% compute()

  # vendor_name munging
  if (tab$vendor_name$type != utf8()) {
    tab <- mutate(tab,
      vendor_name = case_when(
        is.na(vendor_name) ~ NA_character_,
        vendor_name == 1 ~ "CMT",
        vendor_name == 2 ~ "VTS"
        # add DDS
      )
    ) %>% compute()
  }
  confirm_it(tab, "vendor_name", c("CMT", "VTS", "DDS"))

  # rate_code munging
  if (tab$rate_code$type != utf8()) {
    tab <- mutate(tab,
                  rate_code = case_when(
                    is.na(rate_code) ~ NA_character_,
                    rate_code == 1 ~ "Standard rate",
                    rate_code == 2 ~ "JFK",
                    rate_code == 3 ~ "Newark",
                    rate_code == 4 ~ "Nassau or Westchester",
                    rate_code == 5 ~ "Negotiated",
                    rate_code == 6 ~ "Group ride"
                  )
    ) %>% compute()
  } else if (tab$rate_code$type == null()) {
    tab <- mutate(tab, rate_code = NA_character_)  %>% compute()
  }
  confirm_it(tab, "rate_code", c("Standard rate", "JFK", "Newark", "Nassau or Westchester", "Negotiated", "Group ride"))

  # payment_type munging
  if (tab$payment_type$type != utf8()) {
    tab <- mutate(tab,
                  payment_type = case_when(
                    is.na(payment_type) ~ NA_character_,
                    payment_type == 1 ~ "Credit card",
                    payment_type == 2 ~ "Cash",
                    payment_type == 3 ~ "No charge",
                    payment_type == 4 ~ "Dispute",
                    payment_type == 5 ~ "Unknown",
                    payment_type == 6 ~ "Voided trip"
                  )
    ) %>% compute()
  } else {
    tab <- mutate(tab,
                  payment_type = case_when(
                    is.na(payment_type) ~ NA_character_,
                    grepl("credit", payment_type, ignore.case = TRUE) ~ "Credit card",
                    grepl("cash", payment_type, ignore.case = TRUE) ~ "Cash",
                    grepl("no charge", payment_type, ignore.case = TRUE) ~ "No charge",
                    grepl("dispute", payment_type, ignore.case = TRUE) ~ "Dispute",
                    grepl("unknown", payment_type, ignore.case = TRUE) ~ "Unknown",
                    grepl("void", payment_type, ignore.case = TRUE) ~ "Voided trip"
                  )
    ) %>% compute()
  }
  confirm_it(tab, "payment_type", c("Credit card","Cash", "No charge", "Dispute", "Unknown", "Voided trip"))

  # store_and_fwd munging
  if (tab$store_and_fwd$type != utf8()) {
    tab <- mutate(tab,
                  store_and_fwd = case_when(
                    is.na(store_and_fwd) ~ NA_character_,
                    store_and_fwd == 0 ~ "No",
                    store_and_fwd == 1 ~ "Yes"
                  )
    ) %>% compute()
  } else {
    tab <- mutate(tab,
                  store_and_fwd = case_when(
                    is.na(store_and_fwd) ~ NA_character_,
                    store_and_fwd == "N" ~ "No",
                    store_and_fwd == "Y" ~ "Yes"
                  )
    ) %>% compute()
  }
  confirm_it(tab, "store_and_fwd", c("Yes", "No"))

  # mta_tax munging
  if (tab$mta_tax$type == null()) {
    tab <- mutate(tab, mta_tax = NA_real_) %>% compute()
  }

  # Add in any columns that are missing
  for (col in names(proto_schema)) {
    if (!col %in% colnames(tab)) {
      tab[[col]] <- NA
      tab[[col]] <- tab[[col]]$cast(proto_schema[[col]]$type)
    }
  }

  # shuffle and cast to schema (to catch any latent issues)
  tab <- tab %>% select(names(proto_schema)) %>% compute()
  tab <- tab$cast(proto_schema)

  tab %>%
    mutate(
      year = year(pickup_datetime),
      month = month(pickup_datetime),
      day = day(pickup_datetime)
    ) %>%
    # remove improbable dates
    filter(year == file_year & month == file_month) %>%
    write_dataset(out_dir, partitioning = c("year", "month", "day"), min_rows_per_group = 10000L, max_open_files = 3600L)
}

# copy_files(out_dir, s3_bucket("ursa-labs-taxi-data-v2"))
