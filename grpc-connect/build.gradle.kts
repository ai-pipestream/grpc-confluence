plugins {
    `java-library`
}

dependencies {
    implementation(project(":grpc-confluence-api"))
    implementation(project(":grpc-confluence-service"))
    implementation(project(":grpc-microsoft-api"))
    implementation(project(":grpc-microsoft-service"))
    implementation(libs.grpc.netty.shaded)
    implementation(libs.grpc.stub)
    implementation(libs.jackson.databind)
    compileOnly(libs.kafka.connect.api)
    compileOnly(libs.kafka.clients)

    testImplementation(libs.kafka.connect.api)
    testImplementation(libs.kafka.clients)
    testImplementation(libs.grpc.inprocess)
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testRuntimeOnly(libs.junit.launcher)
}

tasks.register<Zip>("connectPluginZip") {
    group = "distribution"
    description = "Kafka Connect plugin zip (drop into plugin.path)"
    archiveClassifier.set("plugin")
    into("grpc-connect") {
        from(tasks.named("jar"))
        from(configurations.runtimeClasspath)
    }
}

tasks.assemble {
    dependsOn("connectPluginZip")
}
