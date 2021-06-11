# KafkaGuru

KafkaGuru is a Desktop GUI application to browse messages on a Kafka cluster.

## Requirements
To build and/or run KafkaGuru, you will need Java-14.
Download the suitable Java bundle for your operating system from https://jdk.java.net/14/, or other vendors as you prefer.

Extract the java archive you downloaded, and set environment variable JAVA_HOME to point to the top-level java directory.

Additionally,, you will also need maven to build the project from source.

## Build
Open a terminal, and change to the project directory (which contains pom.xml)

Run thew following command:
```
mvn compile package
```
This will download the dependencies, build the project, and create a jar file in the target directory.

## Run
```
java -jar target/KafkaGuru-0.3.jar
```
