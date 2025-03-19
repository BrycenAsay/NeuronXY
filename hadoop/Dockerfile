# Using Ubuntu as the base image
FROM apache/hadoop:3.4

# Install Zookeeper
RUN wget -qO- https://archive.apache.org/dist/zookeeper/zookeeper-3.7.0/apache-zookeeper-3.7.0-bin.tar.gz | tar xz -C /opt && \
    mv /opt/apache-zookeeper-3.7.0-bin /opt/zookeeper

# Set environment variables
ENV HADOOP_VERSION=3.4
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$HADOOP_HOME/bin:$PATH
ENV ZOOKEEPER_HOME=/opt/zookeeper
ENV ZOOKEEPER_MYID=1
ENV ZOOKEEPER_SERVERS="zookeeper1","zookeeper2","zookeeper3"

# Copy the Hadoop configuration files into the container
COPY core-site.xml $HADOOP_HOME/etc/hadoop/core-site.xml
COPY hdfs-site.xml $HADOOP_HOME/etc/hadoop/hdfs-site.xml

# Switch back to root
USER root

# Expose ports
EXPOSE 9870 9864 8088 19888 8020 9867 8032 8042

# Copy entrypoint script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Copy Fencing Script
COPY fencing-script.sh /fencing-script.sh
RUN chmod +x /fencing-script.sh

# Add capability to set process priorities
RUN echo '#!/bin/bash\nexit 0' > /bin/nice && chmod +x /bin/nice

# Set the entrypoint
ENTRYPOINT ["/entrypoint.sh"]
