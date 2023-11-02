FROM ubuntu:22.04

RUN apt-get update && apt-get install -y \
    ca-certificates \
    rsync && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists /var/cache/apt/archives

ENV PIKA=/pika \
    PATH=${PIKA}:${PIKA}/bin:${PATH}

WORKDIR ${PIKA}

COPY artifact/pika ${PIKA}/bin/pika
COPY entrypoint.sh /entrypoint.sh
COPY conf/pika.conf ${PIKA}/conf/pika.conf

ENTRYPOINT ["/entrypoint.sh"]

EXPOSE 9221

CMD ["/pika/bin/pika", "-c", "/pika/conf/pika.conf"]