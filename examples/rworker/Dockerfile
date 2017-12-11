FROM rocker/tidyverse

RUN apt-get update &&\
    apt-get install -y libhiredis-dev libzmq3-dev

RUN R --slave -e "devtools::install_github('lecardozo/rworker', ref='development')"
