# Single stage — just copy the pre-built JAR and run it
# We build the JAR locally with Gradle first (./gradlew build)
# then package it into a lightweight container
FROM eclipse-temurin:17-jre

# Run as an unprivileged user, not root: if the server is ever
# compromised, the attacker does not get root inside the container
RUN useradd --system --no-create-home redis \
    && mkdir /data \
    && chown redis:redis /data

# Copy the JAR we already built locally
COPY app/build/libs/app.jar /app/app.jar

USER redis

# The server writes dump.rdb to its working directory. Making that a
# volume keeps the snapshot when the container is removed:
#   docker run -p 6379:6379 -v redis-data:/data redis-from-scratch
WORKDIR /data
VOLUME /data

# Expose Redis port
EXPOSE 6379

# Run the server
CMD ["java", "-jar", "/app/app.jar"]
