FROM eclipse-temurin:21-jdk AS expand-wars

WORKDIR /wars
COPY build/libs/mgmt.war /wars/mgmt/
COPY build/libs/etl.war /wars/etl/
COPY build/libs/engine.war /wars/engine/
COPY build/libs/retrieval.war /wars/retrieval/

RUN cd mgmt && jar -xvf mgmt.war && rm mgmt.war

RUN cp -r mgmt/WEB-INF/lib lib && rm -rf mgmt/WEB-INF/lib

RUN cd etl && jar -xvf etl.war && rm etl.war && rm -rf WEB-INF/lib/*
RUN cd engine && jar -xvf engine.war && rm engine.war && rm -rf WEB-INF/lib/*
RUN cd retrieval && jar -xvf retrieval.war && rm retrieval.war && rm -rf WEB-INF/lib/*

FROM tomcat:9 AS tomcat-base

ENV ARCHAPPL_APPLIANCES=/usr/local/tomcat/archappl_conf/appliances.xml
ENV ARCHAPPL_POLICIES=/usr/local/tomcat/archappl_conf/policies.py
ENV ARCHAPPL_SHORT_TERM_FOLDER=/usr/local/tomcat/storage/sts
ENV ARCHAPPL_MEDIUM_TERM_FOLDER=/usr/local/tomcat/storage/mts
ENV ARCHAPPL_LONG_TERM_FOLDER=/usr/local/tomcat/storage/lts
ENV ARCHAPPL_MYIDENTITY=archappl0
ENV CATALINA_OUT=/dev/stdout

FROM tomcat-base AS copy-webapp
COPY docker/archappl/copy_conf/context.xml /usr/local/tomcat/conf

COPY --from=expand-wars wars/lib /usr/local/tomcat/lib

FROM copy-webapp AS singletomcat
COPY --from=expand-wars wars/mgmt /usr/local/tomcat/webapps/mgmt
COPY --from=expand-wars wars/etl /usr/local/tomcat/webapps/etl
COPY --from=expand-wars wars/engine /usr/local/tomcat/webapps/engine
COPY --from=expand-wars wars/retrieval /usr/local/tomcat/webapps/retrieval

FROM copy-webapp AS mgmt
COPY --from=expand-wars wars/mgmt /usr/local/tomcat/webapps/mgmt

FROM copy-webapp AS etl
COPY --from=expand-wars wars/etl /usr/local/tomcat/webapps/etl

FROM copy-webapp AS engine
COPY --from=expand-wars wars/engine /usr/local/tomcat/webapps/engine

FROM copy-webapp AS retrieval
COPY --from=expand-wars wars/retrieval /usr/local/tomcat/webapps/retrieval

LABEL org.opencontainers.image.source=https://github.com/archiver-appliance/epicsarchiverap
LABEL org.opencontainers.image.description="Docker image for the Archiver Appliance, both as a singletomcat or single war image."