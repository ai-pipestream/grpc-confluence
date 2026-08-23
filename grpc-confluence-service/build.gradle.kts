plugins {
    application
}

dependencies {
    implementation(project(":grpc-confluence-api"))
    implementation(project(":grpc-sync-api"))
    implementation(libs.grpc.netty.shaded)
    implementation(libs.grpc.services)
    implementation(libs.jackson.databind)
    implementation(libs.kafka.clients)
    runtimeOnly(libs.log4j.core)

    testImplementation(project(":grpc-sync-service"))
    testImplementation(libs.grpc.inprocess)
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testRuntimeOnly(libs.junit.launcher)
}

application {
    mainClass = "ai.pipestream.confluence.ConfluenceServer"
}

// The live smoke test hits the real workspace, so it never runs in the
// default test task (and is assumption-gated on credentials as a second guard).
tasks.named<Test>("test") {
    exclude("**/ConfluenceLiveSmokeIT.class")
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
