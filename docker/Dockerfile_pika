FROM ubuntu:22.04 AS builder

LABEL maintainer="SvenDowideit@home.org.au, zhangshaomin_1990@126.com"

ENV PIKA=/pika \
    PIKA_BUILD_DIR=/tmp/pika \
    PATH=${PIKA}:${PIKA}/bin:${PATH} \
    BUILD_TYPE=RelWithDebInfo

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
    autoconf \
    clang-tidy-12

WORKDIR ${PIKA_BUILD_DIR}

COPY . ${PIKA_BUILD_DIR}

RUN  cmake -B ${PIKA_BUILD_DIR}/build -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DUSE_PIKA_TOOLS=OFF
RUN  cmake --build ${PIKA_BUILD_DIR}/build --config ${BUILD_TYPE}

FROM ubuntu:22.04

LABEL maintainer="SvenDwideit@home.org.au, zhangshaomin_1990@126.com"

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

COPY --from=builder ${PIKA_BUILD_DIR}/build/pika ${PIKA}/bin/pika
COPY --from=builder ${PIKA_BUILD_DIR}/entrypoint.sh /entrypoint.sh
COPY --from=builder ${PIKA_BUILD_DIR}/conf/pika.conf ${PIKA}/conf/pika.conf

ENTRYPOINT ["/entrypoint.sh"]

EXPOSE 9221

CMD ["/pika/bin/pika", "-c", "/pika/conf/pika.conf"]
