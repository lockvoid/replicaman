// Kotlin binding over the native Loro core (UniFFI over loro-ffi, see rust/crates/loro-kotlin).
plugins {
    id("replicaman.jvm-library")
    id("replicaman.loro-host-tests")
}

description = "Kotlin binding over the native Loro core"

dependencies {
    compileOnly(libs.jna)
    testImplementation(libs.jna)
    testImplementation(libs.kotlin.test.junit)
    testImplementation(libs.kotlinx.coroutines.test)
}

sourceSets.main {
    resources.srcDir(rootProject.layout.projectDirectory.dir("../build/dependencies/resources"))
}

val cargoManifest = layout.projectDirectory.file("rust/Cargo.toml")
val builtHostLibrary = layout.projectDirectory.file("rust/target/release/${System.mapLibraryName("loro_kotlin")}")

val cargoBuildHost by tasks.registering(Exec::class) {
    description = "Build loro-ffi for this machine with cargo"
    inputs.files(fileTree("rust/src"), "rust/Cargo.toml", "rust/Cargo.lock")
    outputs.file(builtHostLibrary)
    commandLine("cargo", "build", "--locked", "--release", "--features", "cli", "--manifest-path", cargoManifest.asFile.absolutePath)
}

val loroHostLibrary by tasks.registering(Copy::class) {
    description = "The host Loro library the JVM tests load through JNA"
    from(cargoBuildHost)
    into(layout.buildDirectory.dir("host"))
}

tasks.register<Exec>("generateLoroBinding") {
    description = "Regenerate the committed UniFFI Kotlin binding from loro-ffi"
    dependsOn(cargoBuildHost)
    val out = layout.projectDirectory.dir("src/main/kotlin").asFile
    workingDir("rust")
    commandLine(
        "cargo", "run", "--locked", "--features", "cli", "--bin", "uniffi-bindgen", "--",
        "generate", "--library", builtHostLibrary.asFile.absolutePath, "--config", "uniffi.toml",
        "--no-format", "--language", "kotlin", "--out-dir", out.absolutePath,
    )
    doLast {
        val binding = out.resolve("io/replicaman/loro/binding/loro.kt")
        binding.writeText(qualifyCollections(binding.readText()))
    }
}
