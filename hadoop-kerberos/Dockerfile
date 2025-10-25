FROM ubuntu:22.04

# Avoid prompts from apt
ENV DEBIAN_FRONTEND=noninteractive

# Install basic utilities and Java
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    wget \
    curl \
    vim \
    net-tools \
    iputils-ping \
    ssh \
    && rm -rf /var/lib/apt/lists/*

# Install Kerberos packages
RUN apt-get update && apt-get install -y \
    krb5-kdc \
    krb5-admin-server \
    krb5-config \
    krb5-user \
    libpam-krb5 \
    libpam-ccreds \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME - detect architecture
RUN if [ -d "/usr/lib/jvm/java-11-openjdk-arm64" ]; then \
        echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64" >> /etc/environment; \
    elif [ -d "/usr/lib/jvm/java-11-openjdk-amd64" ]; then \
        echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> /etc/environment; \
    fi

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
ENV PATH=$PATH:$JAVA_HOME/bin

# Download and install Hadoop
ARG HADOOP_VERSION=3.3.6
ENV HADOOP_VERSION=${HADOOP_VERSION}
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

RUN wget -q https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar -xzf hadoop-${HADOOP_VERSION}.tar.gz && \
    mv hadoop-${HADOOP_VERSION} ${HADOOP_HOME} && \
    rm hadoop-${HADOOP_VERSION}.tar.gz

# Create directories for Kerberos
RUN mkdir -p /var/kerberos/krb5kdc && \
    mkdir -p /etc/security/keytabs && \
    mkdir -p /var/log

# Create directories for HDFS
RUN mkdir -p /hadoop/dfs/name && \
    mkdir -p /hadoop/dfs/data && \
    mkdir -p /tmp/hadoop && \
    chmod 755 /hadoop/dfs/name && \
    chmod 755 /hadoop/dfs/data && \
    chmod 1777 /tmp/hadoop

# Copy Kerberos configuration files
COPY config/krb5.conf /etc/krb5.conf
COPY config/kdc.conf /var/kerberos/krb5kdc/kdc.conf
COPY config/kadm5.acl /var/kerberos/krb5kdc/kadm5.acl

# Copy Hadoop configuration files with Kerberos settings
COPY config/core-site.xml ${HADOOP_CONF_DIR}/core-site.xml
COPY config/hdfs-site.xml ${HADOOP_CONF_DIR}/hdfs-site.xml

# Set Hadoop environment variables
RUN echo "export JAVA_HOME=$JAVA_HOME" >> ${HADOOP_CONF_DIR}/hadoop-env.sh && \
    echo "export HADOOP_HOME=$HADOOP_HOME" >> ${HADOOP_CONF_DIR}/hadoop-env.sh && \
    echo "export HDFS_NAMENODE_USER=root" >> ${HADOOP_CONF_DIR}/hadoop-env.sh && \
    echo "export HDFS_DATANODE_USER=root" >> ${HADOOP_CONF_DIR}/hadoop-env.sh && \
    echo "export HDFS_SECONDARYNAMENODE_USER=root" >> ${HADOOP_CONF_DIR}/hadoop-env.sh && \
    echo "export HADOOP_OPTS=\"-Djava.security.krb5.conf=/etc/krb5.conf\"" >> ${HADOOP_CONF_DIR}/hadoop-env.sh

# Copy initialization scripts
COPY init-kerberos.sh /usr/local/bin/init-kerberos.sh
COPY start-hdfs-kerberos.sh /usr/local/bin/start-hdfs-kerberos.sh

# Make scripts executable
RUN chmod +x /usr/local/bin/init-kerberos.sh && \
    chmod +x /usr/local/bin/start-hdfs-kerberos.sh

# Expose ports
# 9000: HDFS NameNode IPC
# 9864: DataNode HTTP
# 9866: DataNode data transfer
# 9870: NameNode HTTP
# 88: Kerberos KDC
# 749: Kerberos kadmin
EXPOSE 9000 9864 9866 9870 88 749

# Create entrypoint script
RUN echo '#!/bin/bash\n\
echo "🔐 Initializing Kerberos..."\n\
/usr/local/bin/init-kerberos.sh\n\
echo ""\n\
echo "🚀 Starting HDFS with Kerberos..."\n\
/usr/local/bin/start-hdfs-kerberos.sh\n\
' > /entrypoint.sh && chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]