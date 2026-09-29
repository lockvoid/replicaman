// Optional Loro codec and document APIs over the replication core; the only
// module that imports Loro.
plugins {
    id("replicaman.jvm-library")
    id("replicaman.loro-host-tests")
}

description = "Loro documents for ReplicaMan"

kotlin {
    explicitApi()
}

dependencies {
    api(project(":replicaman"))
    api(project(":loro"))
    // The Android app brings JNA as an @aar through :loro-android; a plain jar beside it is a duplicate class.
    compileOnly(libs.jna)
    testImplementation(libs.jna)
    testImplementation(testFixtures(project(":replicaman")))
    testImplementation(libs.androidx.sqlite)
    testImplementation(libs.kotlin.test.junit)
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(libs.kotlinx.serialization.json)
}
