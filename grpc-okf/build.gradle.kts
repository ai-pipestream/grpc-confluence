plugins {
    `java-library`
}

dependencies {
    implementation(project(":grpc-confluence-api"))
    implementation(project(":grpc-microsoft-api"))
    implementation(project(":grpc-output-spi"))
    implementation(libs.jackson.databind)
    implementation(libs.jackson.dataformat.yaml)
    implementation(libs.protobuf.java)

    testImplementation(project(":grpc-output-filesystem"))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testRuntimeOnly(libs.junit.launcher)
}
