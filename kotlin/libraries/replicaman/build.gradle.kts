// Durable JVM/Android replication core; no Android framework imports.
plugins {
    id("replicaman.jvm-library")
    `java-test-fixtures`
    id("replicaman.kotlin-serialization")
}

description = "ReplicaMan durable replicas and synchronization"

kotlin {
    sourceSets.test {
        kotlin.srcDir("src/test/generated-documents")
        kotlin.srcDir("src/test/resources/generated/dummy/documents")
    }
}

dependencies {
    api(libs.kotlinx.coroutines.core)
    api(libs.kotlinx.serialization.json)
    api(libs.androidx.sqlite)
    implementation(libs.androidx.sqlite.bundled)
    api(libs.okhttp)
    testImplementation(libs.kotlin.test.junit)
    testImplementation(libs.kotlinx.coroutines.test)
}

val javaComponent = components["java"] as AdhocComponentWithVariants
javaComponent.withVariantsFromConfiguration(configurations["testFixturesApiElements"]) { skip() }
javaComponent.withVariantsFromConfiguration(configurations["testFixturesRuntimeElements"]) { skip() }
