# Build stage: compile, run the full test suite, and assemble the
# Confluence proxy distribution.
FROM eclipse-temurin:25-jdk AS build
WORKDIR /src
COPY . .
RUN ./gradlew --no-daemon build :grpc-confluence-service:installDist

FROM eclipse-temurin:25-jre
COPY --from=build /src/grpc-confluence-service/build/install/grpc-confluence-service /opt/confluence
RUN useradd --system --no-create-home confluence
USER confluence
EXPOSE 9095
ENTRYPOINT ["/opt/confluence/bin/grpc-confluence-service"]
