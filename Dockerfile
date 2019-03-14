FROM flink

MAINTAINER mertozen

WORKDIR "/"

COPY target/scala-2.12/flink-project-assembly-0.1-SNAPSHOT.jar /

COPY case.csv /

CMD ["flink run flink-project-assembly-0.1-SNAPSHOT.jar file:///case.csv file:///output/"]
