FROM maven:3.9-eclipse-temurin-17-alpine AS builder

WORKDIR /app
COPY . .


RUN mvn clean package -DskipTests

FROM eclipse-temurin:17-jre-alpine

COPY --from=builder /app/target/*.jar /opt/demo/demo.jar

CMD ["java", "-jar", "/opt/demo/demo.jar"]

EXPOSE 8022 8023

