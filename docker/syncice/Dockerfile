FROM ubuntu:jammy

RUN export DEBIAN_FRONTEND=noninteractive && \
    apt-get -y update && \
    apt-get install -y --no-install-recommends \
        avro-bin \
        rclone \
        s3cmd \
        curl \
        wget \
        tar \
        gzip \
        locales && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    locale-gen en_US.utf8 && update-locale

ARG MINIO_DEB
ARG MC
ARG ICETOOLS_TGZ

COPY syncice_test.sh /
COPY ../../build /build
COPY ../../test/iceberg/warehouse/gperov/test /root/test
RUN ls -R /root && \
    curl -s $MINIO_DEB -o minio.deb && \
    dpkg -i minio.deb && rm minio.deb && \
    curl -s $MC -o /usr/local/bin/mc && \
    chmod +x /usr/local/bin/mc && \
    mkdir -p /root/bin && \
    tar xzf /build/$ICETOOLS_TGZ -C /root && \
    ls /root/bin/syncice && ls /root/bin/hive_metastore_server && ls /root/bin/hive_metastore_client && \
    rm -rf /build

#CMD ["syncice_test.sh"]
