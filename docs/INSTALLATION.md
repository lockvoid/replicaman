# Installation

Add the core client for your platform. Add Loro only if your application synchronizes documents.

The packages are currently consumed from a local checkout. The paths below assume `replicaman` sits beside your application repository. Use the monorepo root for Swift, the crate directory for Rust, and the Rails engine directory for Ruby.

## Swift

Add the local package in Xcode, or declare it in your application's `Package.swift`:

```swift
dependencies: [
    .package(path: "../replicaman"),
],
targets: [
    .target(
        name: "AppData",
        dependencies: [
            .product(name: "ReplicaMan", package: "replicaman"),
        ]
    ),
]
```

The package declares **Swift tools 6.0**, **iOS 18+**, and **macOS 15+**. Xcode resolves the pinned dependencies from the root package.

For document streams, add the `ReplicaManLoro` product to the same target and import `ReplicaManLoro`. A rows-only target does not link the Loro codec.

## Kotlin / Android

The core is a JVM library used by both JVM and Android applications. It uses **Java 17**. To consume the current artifacts, publish to the checkout's local Maven repository:

```sh
# From the ReplicaMan repository root.
./gradlew :replicaman:publishAllPublicationsToStagingRepository
```

Add that repository in your application's `settings.gradle.kts`, alongside your existing repositories:

```kotlin
dependencyResolutionManagement {
    repositories {
        google()
        mavenCentral()
        maven { url = uri("../replicaman/build/maven") }
    }
}
```

Then add the core dependency:

```kotlin
dependencies {
    implementation("io.replicaman:replicaman:0.1.0")
}
```

### Add Loro

Build and stage the optional native packages from the monorepo:

```sh
# JVM/macOS or JVM/Linux host library.
kotlin/gradlew -p kotlin :loro:loroHostLibrary
./gradlew :loro:publishAllPublicationsToStagingRepository \
  :replicaman-loro:publishAllPublicationsToStagingRepository

# Android arm64-v8a; requires the Android SDK and NDK.
kotlin/gradlew -p kotlin -Pandroid=true :loro-android:cargoBuildAndroid
./gradlew -Pandroid=true :loro-android:publishAllPublicationsToStagingRepository
```

For Android:

```kotlin
dependencies {
    implementation("io.replicaman:replicaman:0.1.0")
    implementation("io.replicaman:replicaman-loro:0.1.0")
    implementation("io.replicaman:loro-android:0.1.0")
}
```

The Loro AAR currently targets **Android API 33+**, **arm64-v8a**. Its build uses NDK **28.2.13676358**. The AAR brings the Android JNA dependency; do not add a second plain JNA jar to that application.

For a JVM application, use `replicaman-loro`, add `net.java.dev.jna:jna:5.19.1`, and set `jna.library.path` to the built `kotlin/libraries/loro/build/host` directory. The JVM jar does not bundle desktop native libraries.

## Rust

```toml
[dependencies]
replicaman = { path = "../replicaman/rust/crates/replicaman" }
```

The crate declares **Rust 1.96+**, edition **2024**. For documents, add the Loro codec crate:

```toml
[dependencies]
replicaman = { path = "../replicaman/rust/crates/replicaman" }
replicaman-loro = { path = "../replicaman/rust/crates/replicaman-loro" }
```

Your application provides an executor and an HTTP adapter. The [setup guide](./SETUP.md#rust) shows the executor boundary; the core does not choose a runtime for you.

## Rails

```ruby
# Gemfile
gem 'replica_man', path: '../replicaman/ruby'

# Add for document streams.
gem 'loro', path: '../replicaman/ruby/vendor/loro'
```

```sh
bundle install
bin/rails replica_man:install:migrations
bin/rails db:migrate
```

The engine requires **Rails 8+**, **Ruby 3.4+**, and **PostgreSQL**. The optional Ruby Loro extension requires **Ruby 4.0** and a Rust toolchain to compile its native extension.

Configure authentication, the dataset epoch, and streams before serving the endpoint. See [Rails server](./SERVER.md).

## Code generator

The generator uses Ruby's standard library and requires **Ruby 3.4+**:

```sh
ruby codegen/bin/replica-codegen --help
```

Generate all clients from one committed manifest. [Models and code generation](./MODELS.md) covers exporting that manifest and checking generated output.

## Next steps

- [Rails server](./SERVER.md) — define the server side.
- [Setup](./SETUP.md) — connect the client and open its store.
- [Notes example](../examples/notes/README.md) — try local persistence without a server.
