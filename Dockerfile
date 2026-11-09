# syntax=docker/dockerfile:1
# Build stage: compile, run the full test suite, and assemble the
# Confluence proxy distribution. Builds against the published
# ai.pipestream.integration snapshots, or against a local grpc-integration
# checkout passed as a named build context (it replaces the empty
# "integration" stage below):
#   docker build --build-context integration=../grpc-integration -f Dockerfile .
FROM scratch AS integration

FROM eclipse-temurin:25-jdk AS build
ARG GRADLE_MAX_WORKERS=2
ENV JAVA_TOOL_OPTIONS="-XX:ActiveProcessorCount=2"
COPY --from=integration . /src/grpc-integration
WORKDIR /src/grpc-confluence
COPY . .
RUN --mount=type=cache,target=/root/.gradle ./gradlew --no-daemon --max-workers=${GRADLE_MAX_WORKERS} build :grpc-confluence-service:installDist && \
    find . -path '*/build/test-results/test/TEST-*.xml' -exec awk -F '"' \
    '/<testsuite / { for (i=1; i<NF; i++) { if ($i ~ / tests=$/) tests+=$(i+1); if ($i ~ / failures=$/) failures+=$(i+1); if ($i ~ / errors=$/) errors+=$(i+1); if ($i ~ / skipped=$/) skipped+=$(i+1) } } END { printf "CONTAINER_TEST_TOTAL tests=%d failures=%d errors=%d skipped=%d\n", tests, failures, errors, skipped; if (tests == 0 || failures != 0 || errors != 0) exit 1 }' {} +

FROM eclipse-temurin:25-jre
COPY --from=build /src/grpc-confluence/grpc-confluence-service/build/install/grpc-confluence-service /opt/confluence
USER 65532:65532
EXPOSE 9095
ENTRYPOINT ["/opt/confluence/bin/grpc-confluence-service"]
