FROM apache/spark:4.1.2-python3

USER spark

WORKDIR /opt/spark-apps

COPY app/ ./app/
COPY conf/spark-defaults.conf /opt/spark/conf/