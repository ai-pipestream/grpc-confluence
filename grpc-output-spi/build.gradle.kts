plugins {
    `java-library`
}

dependencies {
    api(project(":grpc-confluence-api"))
    api(project(":grpc-microsoft-api"))
    api(libs.protobuf.java)
    implementation(libs.protobuf.java.util)

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testRuntimeOnly(libs.junit.launcher)
}
