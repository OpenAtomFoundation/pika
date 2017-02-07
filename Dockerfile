FROM centos:latest
MAINTAINER left2right <yqzhang@easemob.com>

RUN yum -y update && \
    yum -y install snappy-devel && \
    yum -y install protobuf-compiler && \
    yum -y install protobuf-devel && \
    yum -y install bzip2-devel && \
    yum -y install zlib-devel && \
    yum -y install bzip2 && \
    yum -y install gcc-c++ && \
    yum -y install make && \
    yum -y install git

ENV PIKA  /pika
COPY . ${PIKA}
WORKDIR ${PIKA}
RUN git submodule init
RUN git submodule update
RUN make __REL=1
RUN cp -f ${PIKA}/output/lib/libglog.so.0 /usr/lib64/
ENV PATH   ${PIKA}/output/bin:${PATH}

WORKDIR ${PIKA}/output
