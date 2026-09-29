plugins {
    id("replicaman.kotlin-jvm")
    application
}

dependencies {
    implementation(project(":replicaman"))
}

application {
    mainClass.set("example.MainKt")
}
