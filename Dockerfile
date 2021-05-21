 FROM centos:7 as builder

LABEL maintainer="SvenDowideit@home.org.au, zhangshaomin_1990@126.com"

ENV PIKA  /pika
ENV PIKA_BUILD_DIR /tmp/pika
ENV PATH ${PIKA}:${PIKA}/bin:${PATH}

COPY . ${PIKA_BUILD_DIR}

WORKDIR ${PIKA_BUILD_DIR}

RUN rpm -ivh https://mirrors.aliyun.com/epel/epel-release-latest-7.noarch.rpm && \
    yum clean all && \
    yum -y makecache && \
    yum -y install snappy-devel && \
    yum -y install protobuf-devel && \
    yum -y install gflags-devel && \
    yum -y install glog-devel && \
    yum -y install bzip2-devel && \
    yum -y install zlib-devel && \
    yum -y install lz4-devel && \
    yum -y install libzstd-devel && \
    yum -y install gcc-c++ && \
    yum -y install make && \
    yum -y install which && \
    yum -y install git && \
    make -j$(shell grep -c ^processor /proc/cpuinfo 2>/dev/null) && \
    cp -r ${PIKA_BUILD_DIR}/output ${PIKA} && \
    cp -r ${PIKA_BUILD_DIR}/entrypoint.sh ${PIKA} && \
    yum -y remove gcc-c++ && \
    yum -y remove make && \
    yum -y remove which && \
    yum -y remove git && \
    yum -y clean all 

FROM centos:7
ENV PIKA  /pika
ENV PATH ${PIKA}:${PIKA}/bin:${PATH}

RUN set -eux; yum install -y epel-release; \
 yum install -y snappy protobuf gflags glog bzip2 zlib lz4 libzstd rsync; \
 yum clean all;

WORKDIR ${PIKA}
COPY --from=builder $PIKA ./

ENTRYPOINT ["/pika/entrypoint.sh"]
CMD ["/pika/bin/pika", "-c", "/pika/conf/pika.conf"]
