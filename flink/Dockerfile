FROM flink:1.20.0-scala_2.12-java17

# Connecteurs Kafka et JDBC + driver Postgres (curl et envsubst sont déjà dans l'image)
ARG MAVEN=https://repo1.maven.org/maven2
RUN wget -q -P /opt/flink/lib \
      ${MAVEN}/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar \
      ${MAVEN}/org/apache/flink/flink-connector-jdbc/3.2.0-1.19/flink-connector-jdbc-3.2.0-1.19.jar \
      ${MAVEN}/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar

COPY sql /opt/sql
COPY submit.sh /opt/submit.sh
RUN chmod +x /opt/submit.sh

# Le volume des checkpoints hérite de ces droits à sa création
RUN mkdir -p /flink-data/checkpoints && chown -R flink:flink /flink-data
