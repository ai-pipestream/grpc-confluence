plugins {
    `java-library`
}

dependencies {
    api(project(":grpc-output-spi"))

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testRuntimeOnly(libs.junit.launcher)
}
