FROM r-base

ADD install.R /

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libssl-dev libcurl4-openssl-dev \
        libhiredis-dev libzmq3-dev && \
    groupadd -r rworker && \
    useradd --no-log-init -r -g rworker rworker

RUN Rscript /install.R

USER rworker
