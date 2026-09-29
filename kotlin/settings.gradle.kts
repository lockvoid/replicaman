pluginManagement {
    includeBuild("build-logic")
    repositories {
        google()
        mavenCentral()
        gradlePluginPortal()
    }
}

plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        google()
        mavenCentral()
    }
}

rootProject.name = "replicaman"

include(":replicaman")
include(":replicaman-loro")
include(":loro")
include(":e2e-worker")
include(":notes-example")
project(":replicaman").projectDir = file("libraries/replicaman")
project(":replicaman-loro").projectDir = file("libraries/replicaman-loro")
project(":loro").projectDir = file("libraries/loro")
project(":notes-example").projectDir = file("samples/notes")

// The AAR needs an Android SDK; JVM-only builds and CI leave it out.
if (providers.gradleProperty("android").orNull == "true" || providers.environmentVariable("ANDROID_HOME").isPresent) {
    include(":loro-android")
    project(":loro-android").projectDir = file("libraries/loro-android")
}
