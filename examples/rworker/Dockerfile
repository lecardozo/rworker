FROM rocker/tidyverse

RUN apt-get update &&\
    apt-get install -y libhiredis-dev

RUN R --slave -e "devtools::install_github('lecardozo/rworker', ref='development')"
