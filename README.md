
# Larger-Than-Memory Data Workflows with Apache Arrow

<!-- badges: start -->
[![DOI](https://zenodo.org/badge/505020662.svg)](https://zenodo.org/badge/latestdoi/505020662) [![Netlify Status](https://api.netlify.com/api/v1/badges/ae31113f-79b7-4bc0-9e26-600ced0da14b/deploy-status)](https://app.netlify.com/sites/arrow-user2022/deploys)
<!-- badges: end -->

This repository contains source code and data for the Apache Arrow workshop run as part of the 2022 UseR! Conference. You can fork and download this repository from [GitHub](https://github.com/) with:

``` r
usethis::create_from_github("djnavarro/arrow-user2022", destdir="<your chosen path>")
```

The repository is not an R package, but it does have a DESCRIPTION file. To install all the package dependencies associated with the workshop, open R in the project folder and use this:

``` r
remotes::install_deps()
```

The repository contains almost everything you need for the workshop. However, it does not include a copy of the data sets due to file size issues. 

For the full NYC taxi data, see the instructions on the website. For the "tiny taxi" data, you can download directly from GitHub:

``` r
download.file(
  url = "https://github.com/djnavarro/arrow-user2022/releases/download/v0.1/nyc-taxi-tiny.zip",
  destfile = here::here("data/nyc-taxi-tiny.zip")
)
```

To extract the parquet files from the archive:

``` r
unzip(
  zipfile = here::here("data/nyc-taxi-tiny.zip"), 
  exdir = here::here("data")
)
```

The workshop website files are contained within the `_site` folder, and are online at https://arrow-user2022.netlify.app/

