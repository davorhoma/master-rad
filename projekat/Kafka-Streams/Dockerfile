FROM maven:3.9-eclipse-temurin-21 AS maven_build
WORKDIR /app
COPY pom.xml .
# Preuzimanje zavisnosti unapred (keširanje sloja)
RUN mvn dependency:go-offline

COPY . .
RUN mvn clean package

FROM eclipse-temurin:21-jre
WORKDIR /app
# Kopiramo ceo target folder iz build faze
COPY --from=maven_build /app/target /app/target

# Komanda više nije fiksna ovde, nju ćemo kontrolisati kroz docker-compose preko 'command'

# Stara verzija
# FROM maven AS maven_build

# COPY pom.xml /usr/pom.xml
# RUN mvn -f /usr/pom.xml clean package

# COPY . /usr/
# RUN mvn -f /usr/pom.xml clean package

# FROM eclipse-temurin:21-jre
# COPY --from=maven_build /usr/target /usr
# CMD ["java", "-cp", "/usr/kafka-streams-demo-1.0-SNAPSHOT.jar:/usr/target/libraries/*", "com.example.kafka_streams_examples.SocialMediaProcessor"]