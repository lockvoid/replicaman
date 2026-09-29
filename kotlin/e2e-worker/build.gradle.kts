plugins {
    id("replicaman.kotlin-jvm")
    application
}

dependencies {
    implementation(project(":replicaman-loro"))
    implementation(libs.jna)
    implementation(libs.kotlinx.coroutines.core)
    implementation(libs.kotlinx.serialization.json)
    implementation(libs.androidx.sqlite)
    implementation(libs.okhttp)
}

application {
    mainClass.set("conformance.WorkerKt")
}
