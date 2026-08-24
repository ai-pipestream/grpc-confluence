plugins {
    application
}

dependencies {
    implementation(project(":grpc-confluence-api"))
    implementation(project(":grpc-microsoft-api"))
    implementation(project(":grpc-sync-api"))
    implementation(libs.grpc.netty.shaded)
    implementation(libs.grpc.stub)
    implementation(libs.jackson.databind)
    implementation(libs.mcp.core)
    implementation(libs.mcp.json.jackson2)
    runtimeOnly(libs.slf4j.jdk14)
    runtimeOnly(libs.log4j.core)
    compileOnly(libs.tomcat.annotations)

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testImplementation(project(":grpc-sync-service"))
    testImplementation(libs.grpc.inprocess)
    testRuntimeOnly(libs.junit.launcher)
}

application {
    mainClass = "ai.pipestream.mcp.McpServerMain"
}
