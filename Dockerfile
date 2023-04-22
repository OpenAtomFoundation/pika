FROM ubuntu:22.04 AS builder

LABEL maintainer="SvenDowideit@home.org.au, zhangshaomin_1990@126.com"

ENV PIKA=/pika \
    PIKA_BUILD_DIR=/tmp/pika \
    PATH=${PIKA}:${PIKA}/bin:${PATH}

ARG ENABLE_PROXY=false

RUN if [ "$ENABLE_PROXY" = "true" ] ; \
    then sed -i 's/http:\/\/archive.ubuntu.com/http:\/\/mirrors.aliyun.com/g' /etc/apt/sources.list ; \
         sed -i 's/http:\/\/ports.ubuntu.com/http:\/\/mirrors.aliyun.com/g' /etc/apt/sources.list ; \
    fi 

RUN apt-get update && apt-get install -y \
    ca-certificates \
    build-essential \
    git \
    cmake \
    autoconf

WORKDIR ${PIKA_BUILD_DIR}

COPY . ${PIKA_BUILD_DIR}

RUN ${PIKA_BUILD_DIR}/build.sh

FROM ubuntu:22.04

ARG ENABLE_PROXY=false

RUN if [ "$ENABLE_PROXY" = "true" ] ; \
    then sed -i 's/http:\/\/archive.ubuntu.com/http:\/\/mirrors.aliyun.com/g' /etc/apt/sources.list ; \
         sed -i 's/http:\/\/ports.ubuntu.com/http:\/\/mirrors.aliyun.com/g' /etc/apt/sources.list ; \
    fi 

RUN apt-get update && apt-get install -y \
    ca-certificates \
    rsync && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists /var/cache/apt/archives

ENV PIKA=/pika \
    PIKA_BUILD_DIR=/tmp/pika \
    PATH=${PIKA}:${PIKA}/bin:${PATH}

WORKDIR ${PIKA}

COPY --from=builder ${PIKA_BUILD_DIR}/output/pika ${PIKA}/bin/pika
COPY --from=builder ${PIKA_BUILD_DIR}/conf/pika.conf ${PIKA}/conf/pika.conf

EXPOSE 9221

CMD ["/pika/bin/pika", "-c", "/pika/conf/pika.conf"]
