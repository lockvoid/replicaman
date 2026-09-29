plugins {
    id("replicaman.kotlin-jvm")
    `java-library`
    `maven-publish`
}

group = "io.replicaman"
version = rootProject.projectDir.resolve("../VERSION").readText().trim()

java {
    withSourcesJar()
}

tasks.withType<Test>().configureEach {
    useJUnit()
    systemProperty("replicaman.root", rootProject.projectDir.parentFile.absolutePath)
    testLogging { events("failed") }
}

publishing {
    publications {
        create<MavenPublication>("library") {
            from(components["java"])
            pom {
                name.set(project.name)
                description.set(provider { project.description })
                licenses {
                    license {
                        name.set("MIT")
                        url.set("https://opensource.org/licenses/MIT")
                    }
                }
            }
        }
    }
    repositories {
        maven {
            name = "staging"
            url = rootProject.layout.buildDirectory.dir("maven").get().asFile.toURI()
        }
    }
}
