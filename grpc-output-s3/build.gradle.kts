plugins {
    `java-library`
}

dependencies {
    api(project(":grpc-output-spi"))
    implementation(libs.awssdk.s3)

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testRuntimeOnly(libs.junit.launcher)
}
