FROM centos:latest
MAINTAINER left2right <yqzhang@easemob.com>

RUN yum -y update && \
    yum -y install snappy-devel && \
    yum -y install protobuf-devel && \
    yum -y install gcc-c++ && \
    yum -y install make && \
    yum -y install git

ENV PIKA  /pika
COPY . ${PIKA}
WORKDIR ${PIKA}
RUN make RPATH=${PIKA}/third/glog/.libs
ENV PATH   ${PIKA}/output/bin:${PATH}

WORKDIR ${PIKA}/output
