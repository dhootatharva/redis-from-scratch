# Single stage — just copy the pre-built JAR and run it
# We build the JAR locally with Gradle first
# then package it into a lightweight container
FROM eclipse-temurin:17-jre

WORKDIR /app

# Copy the JAR we already built locally
COPY app/build/libs/app.jar app.jar

# Expose Redis port
EXPOSE 6379

# Run the server
CMD ["java", "-jar", "app.jar"]