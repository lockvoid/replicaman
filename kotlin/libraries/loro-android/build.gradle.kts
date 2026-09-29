// The arm64 .so payload for :loro plus the JNA aar; cargoBuildAndroid builds the .so.
plugins {
    id("replicaman.android-library")
}

android {
    publishing { singleVariant("release") { withSourcesJar() } }
    namespace = "io.replicaman.loro.binding.android"
    compileSdk = 37
    compileSdkMinor = 2

    defaultConfig {
        minSdk = 33
        ndk { abiFilters += listOf("arm64-v8a") }
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }

    packaging {
        jniLibs { useLegacyPackaging = false }
    }
}

dependencies {
    api(project(":loro"))
    implementation("${libs.jna.get()}@aar")
}

val jniLibs = layout.projectDirectory.dir("src/main/jniLibs")

val cargoBuildAndroid by tasks.registering(Exec::class) {
    description = "Build loro-ffi for arm64-v8a with cargo-ndk"
    val crate = rootProject.layout.projectDirectory.dir("libraries/loro/rust")
    inputs.files(crate.dir("src").asFileTree, crate.file("Cargo.toml"), crate.file("Cargo.lock"))
    outputs.file(jniLibs.file("arm64-v8a/libloro_kotlin.so"))
    workingDir(crate)
    environment("RUSTFLAGS", "-C link-arg=-Wl,-z,max-page-size=16384")
    commandLine("cargo", "ndk", "-t", "arm64-v8a", "-o", jniLibs.asFile.absolutePath, "build", "--locked", "--release")
    val ndk = providers.environmentVariable("ANDROID_NDK_HOME")
    val library = jniLibs.file("arm64-v8a/libloro_kotlin.so").asFile
    doLast {
        val host = if (System.getProperty("os.name").startsWith("Mac")) "darwin-x86_64" else "linux-x86_64"
        val readelf = File(ndk.get(), "toolchains/llvm/prebuilt/$host/bin/llvm-readelf")
        verifyPageAlignment(readelf, library)
    }
}

tasks.named("preBuild") { dependsOn(cargoBuildAndroid) }
afterEvaluate {
    publishing {
        publications { create<MavenPublication>("library") { from(components["release"]) } }
        repositories { maven { name = "staging"; url = rootProject.layout.buildDirectory.dir("maven").get().asFile.toURI() } }
    }
}
