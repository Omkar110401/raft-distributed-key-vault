FROM eclipse-temurin:17-jdk

WORKDIR /app

# Copy project
COPY . /app

# Build jar (skip tests to speed up local iteration)
RUN ./gradlew clean bootJar -x test --no-daemon

EXPOSE 8080

# Run with provided env vars (NODE_ID, SERVER_PORT)
ENTRYPOINT ["sh","-c","java -jar build/libs/*.jar"]
