FROM apache/hadoop:3.4.3 AS hadoop
FROM apache/hive:4.0.0 AS hive

FROM quay.io/jupyter/pyspark-notebook:latest

USER root
COPY --from=hadoop /opt/hadoop /opt/hadoop
COPY --from=hive /opt/hive /opt/hive
ENV HADOOP_HOME=/opt/hadoop
ENV HIVE_HOME=/opt/hive
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV PATH=/opt/hadoop/bin:/opt/hadoop/sbin:/opt/hive/bin:${PATH}
RUN pip install --no-cache-dir pymongo==4.11.3 cassandra-driver==3.29.2 requests==2.32.3
USER ${NB_UID}
WORKDIR /home/jovyan/work
