# Build stage: compile, run the full test suite, and assemble the
# Confluence proxy distribution. Builds against the published
# ai.pipestream.integration snapshots, or against a local grpc-integration
# checkout passed as a named build context (it replaces the empty
# "integration" stage below):
#   docker build --build-context integration=../grpc-integration -f Dockerfile .
FROM scratch AS integration

FROM eclipse-temurin:25-jdk AS build
COPY --from=integration . /src/grpc-integration
WORKDIR /src/grpc-confluence
COPY . .
RUN ./gradlew --no-daemon build :grpc-confluence-service:installDist

FROM eclipse-temurin:25-jre
COPY --from=build /src/grpc-confluence/grpc-confluence-service/build/install/grpc-confluence-service /opt/confluence
RUN useradd --system --no-create-home confluence
USER confluence
EXPOSE 9095
ENTRYPOINT ["/opt/confluence/bin/grpc-confluence-service"]
