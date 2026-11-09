plugins {
    `java-library`
    `java-test-fixtures`
}

dependencies {
    implementation(project(":grpc-output-spi"))
    implementation(libs.jackson.databind)
    implementation(libs.jackson.dataformat.yaml)
    api(libs.protobuf.java)

    testImplementation(project(":grpc-output-filesystem"))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testRuntimeOnly(libs.junit.launcher)
}
