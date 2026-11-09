plugins {
    `java-library`
}

dependencies {
    api(libs.protobuf.java)
    implementation(libs.protobuf.java.util)

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testRuntimeOnly(libs.junit.launcher)
}
