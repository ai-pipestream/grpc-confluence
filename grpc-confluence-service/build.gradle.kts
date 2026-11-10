plugins {
    application
}

dependencies {
    implementation(project(":grpc-confluence-api"))
    implementation(libs.integration.sync.api)
    implementation(libs.integration.okf)
    implementation(project(":grpc-confluence-output"))
    implementation(libs.integration.output.spi)
    runtimeOnly(libs.integration.output.filesystem)
    runtimeOnly(libs.integration.output.s3)
    implementation(libs.grpc.netty.shaded)
    implementation(libs.grpc.services)
    implementation(libs.jackson.databind)
    implementation(libs.kafka.clients)
    runtimeOnly(libs.log4j.core)

    testImplementation(libs.integration.sync.service)
    testImplementation(project(":grpc-confluence-mcp"))
    testImplementation(libs.mcp.core)
    testImplementation(libs.integration.output.filesystem)
    testImplementation(libs.integration.output.s3)
    testImplementation(libs.grpc.inprocess)
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testRuntimeOnly(libs.junit.launcher)
}

application {
    mainClass = "ai.pipestream.confluence.ConfluenceServer"
}

// The live smoke and coverage tests hit the real workspace, so they never run
// in the default test task (and are assumption-gated on credentials as a second guard).
tasks.named<Test>("test") {
    exclude("**/ConfluenceLiveSmokeIT.class")
    exclude("**/ConfluenceLiveCoverageIT.class")
}

tasks.register<Test>("liveSmokeTest") {
    description = "One cheap read against the real Confluence workspace; needs CONFLUENCE_EMAIL + CONFLUENCE_API_TOKEN (or the CONFLUENCE_USER / CONFLUENCE_TOKEN aliases), skips otherwise"
    group = "verification"
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    filter {
        includeTestsMatching("ai.pipestream.confluence.ConfluenceLiveSmokeIT")
    }
}

tasks.register<Test>("liveCoverageTest") {
    description = "Calls every ConfluenceService rpc against the real Confluence workspace and writes build/live-coverage.md; needs CONFLUENCE_EMAIL + CONFLUENCE_API_TOKEN (or the CONFLUENCE_USER / CONFLUENCE_TOKEN aliases), optional CONFLUENCE_SPACES, skips otherwise"
    group = "verification"
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    filter {
        includeTestsMatching("ai.pipestream.confluence.ConfluenceLiveCoverageIT")
    }
    systemProperty("liveCoverage.report", layout.buildDirectory.file("live-coverage.md").get().asFile.path)
    // A live run is never up to date, and the report goes to stdout too.
    outputs.upToDateWhen { false }
    testLogging {
        showStandardStreams = true
    }
}
