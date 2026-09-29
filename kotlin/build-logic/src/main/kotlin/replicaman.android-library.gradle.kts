plugins {
    id("com.android.library")
    `maven-publish`
}

group = "io.replicaman"
version = rootProject.projectDir.resolve("../VERSION").readText().trim()
