# Build context is services/ledger:
#   docker build -t wallet-ledger/ledger services/ledger

# ---- build stage: Maven + JDK ----
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
# Download dependencies first, so this layer stays cached until pom.xml changes.
COPY pom.xml .
RUN mvn -q dependency:go-offline
COPY src ./src
RUN mvn -q -DskipTests package

# ---- runtime stage: JRE only (no compiler, no Maven), non-root ----
FROM eclipse-temurin:21-jre-alpine
RUN addgroup -S app && adduser -S app -G app
WORKDIR /app
COPY --from=build /src/target/ledger.jar /app/ledger.jar
USER app
EXPOSE 9090
# Use up to 75% of the container's memory limit for the heap.
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/ledger.jar"]