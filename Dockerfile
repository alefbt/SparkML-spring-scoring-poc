FROM openjdk:8-jdk-alpine
VOLUME /tmp

#
# Issue with xerial.snappy
#
RUN apk update && apk add --no-cache libc6-compat
RUN ln -s /lib64/ld-linux-x86-64.so.2 /lib/ld-linux-x86-64.so.2
ENV LD_LIBRARY_PATH /lib64

ADD /src/main/resources/data/pipelines-store /pipelines-store
COPY target/mlserver.jar mlserver.jar

EXPOSE 8080

ENTRYPOINT ["java", "-Dspark.ui.enabled=false", "-Dpipelines.folder=/pipelines-store","-Djava.security.egd=file:/dev/./urandom","-jar","/mlserver.jar"]
