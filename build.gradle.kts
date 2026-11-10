import org.gradle.external.javadoc.StandardJavadocDocletOptions

// JDK 24+ warns when a library touches sun.misc.Unsafe (protobuf-java's
// UnsafeUtil) or loads native code (grpc-netty-shaded). Both are deliberate
// in those libraries, so the services and tests grant them explicitly.
val runtimeJvmArgs = listOf(
    "--enable-native-access=ALL-UNNAMED",
    "--sun-misc-unsafe-memory-access=allow",
)

// Snapshots publish to the ai.pipestream Forgejo registry (same convention
// as protomolt). Reading it needs no credentials; publishing needs
// FORGEJO_TOKEN (and optionally FORGEJO_ACTOR).
val registryUrl = "https://git.rokkon.com/api/packages/ai-pipestream/maven"
val repoUrl = "https://github.com/ai-pipestream/grpc-confluence"

subprojects {
    group = "ai.pipestream.confluence"
    version = "0.1.0-SNAPSHOT"

    plugins.withType<JavaPlugin> {
        apply(plugin = "maven-publish")

        // grpc-java pulls an older Guava whose AbstractFuture calls the
        // terminally deprecated sun.misc.Unsafe on JDK 25; newer Guava uses
        // VarHandle instead.
        dependencies {
            constraints {
                add("implementation", "com.google.guava:guava:33.8.0-jre")
            }
        }
        extensions.configure<JavaPluginExtension> {
            toolchain {
                languageVersion = JavaLanguageVersion.of(25)
            }
            withJavadocJar()
            withSourcesJar()
        }
        tasks.withType<Test>().configureEach {
            useJUnitPlatform()
            jvmArgs(runtimeJvmArgs)
            testLogging {
                events("failed", "skipped")
                showStackTraces = true
            }
        }
        tasks.withType<Javadoc>().configureEach {
            // Generated protobuf contracts are not our comments.
            exclude("**/ai/pipestream/confluence/v1/**")
            options.encoding = "UTF-8"
            (options as StandardJavadocDocletOptions).apply {
                addBooleanOption("Xdoclint:all", true)
                addBooleanOption("Xwerror", true)
                memberLevel = JavadocMemberLevel.PROTECTED
            }
        }
        // Every compiler warning fails the build: deprecation, unchecked,
        // rawtypes and the rest of -Xlint:all.
        tasks.withType<JavaCompile>().configureEach {
            options.encoding = "UTF-8"
            options.compilerArgs.addAll(listOf("-Xlint:all", "-Werror"))
        }
        plugins.withType<ApplicationPlugin> {
            extensions.configure<JavaApplication> {
                applicationDefaultJvmArgs = runtimeJvmArgs
            }
        }
        tasks.named("check") {
            dependsOn("javadoc")
        }

        extensions.configure<PublishingExtension> {
            repositories {
                val forgejoToken = System.getenv("FORGEJO_TOKEN")
                if (version.toString().endsWith("-SNAPSHOT") && !forgejoToken.isNullOrBlank()) {
                    maven {
                        name = "Registry"
                        url = uri(registryUrl)
                        credentials {
                            username = System.getenv("FORGEJO_ACTOR") ?: "ci"
                            password = forgejoToken
                        }
                    }
                }
            }
            publications {
                create<MavenPublication>("maven") {
                    from(components["java"])
                    pom {
                        name = project.name
                        description = providers.provider { project.description ?: project.name }
                        url = repoUrl
                        licenses {
                            license {
                                name = "MIT License"
                                url = "https://opensource.org/license/mit"
                            }
                        }
                        scm {
                            connection = "scm:git:$repoUrl.git"
                            url = repoUrl
                        }
                    }
                }
            }
        }
    }
}
