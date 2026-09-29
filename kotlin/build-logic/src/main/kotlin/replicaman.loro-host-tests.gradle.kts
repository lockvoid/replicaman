// Tests that load the native Loro core through JNA; :loro builds it with cargo.
val hostLibrary = rootProject.layout.projectDirectory
    .file("libraries/loro/build/host/${System.mapLibraryName("loro_kotlin")}")

tasks.withType<Test>().configureEach {
    dependsOn(":loro:loroHostLibrary")
    val library = hostLibrary.asFile
    inputs.files(library).withPropertyName("loroHostLibrary")
    systemProperty("jna.library.path", library.parent)
}
