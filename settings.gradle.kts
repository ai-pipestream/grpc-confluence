plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "grpc-confluence"

include("grpc-confluence-api")
include("grpc-confluence-service")
include("grpc-confluence-output")
include("grpc-confluence-mcp")
include("grpc-confluence-connect")

// The shared core (ai.pipestream.integration:*) builds from source when a
// grpc-integration checkout is next to this one (or at -PintegrationDir);
// Gradle substitutes it for the published snapshots. Without a checkout the
// snapshots resolve from the ai.pipestream Forgejo registry.
val integrationDir = file(providers.gradleProperty("integrationDir").getOrElse("../grpc-integration"))
if (integrationDir.resolve("settings.gradle.kts").isFile) {
    includeBuild(integrationDir)
}

dependencyResolutionManagement {
    repositories {
        mavenCentral()
        maven {
            name = "PipestreamRegistry"
            url = uri("https://git.rokkon.com/api/packages/ai-pipestream/maven")
            // Only the core comes from here; everything else is Central.
            content {
                includeGroup("ai.pipestream.integration")
            }
        }
    }
}
