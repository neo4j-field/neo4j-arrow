FROM gradle:7.1-jdk11 AS build
USER gradle
WORKDIR /home/gradle/
COPY --chown=gradle:gradle . /home/gradle/
RUN gradle build --no-daemon
