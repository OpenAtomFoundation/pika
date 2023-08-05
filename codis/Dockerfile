FROM golang:1.20 AS builder

WORKDIR /codis
COPY . /codis

RUN make build-all

FROM ubuntu:22.04

ARG ENABLE_PROXY=false

RUN if [ "$ENABLE_PROXY" = "true" ] ; \
    then sed -i 's/http:\/\/archive.ubuntu.com/http:\/\/mirrors.aliyun.com/g' /etc/apt/sources.list ; \
         sed -i 's/http:\/\/ports.ubuntu.com/http:\/\/mirrors.aliyun.com/g' /etc/apt/sources.list ; \
    fi 

RUN apt-get update && \
    apt-get install -y ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /codis/bin /codis/bin
COPY --from=builder /codis/config /codis/config
COPY --from=builder /codis/scripts /codis/scripts
COPY --from=builder /codis/admin /codis/admin

WORKDIR /codis
