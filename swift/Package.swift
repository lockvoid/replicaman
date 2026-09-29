// swift-tools-version: 6.0
import PackageDescription

/// `ReplicaMan` — the replication engine, client half. Keeps a device replica
/// convergent with the Rails server (`ruby`): pull frames by
/// shard cursor into an atomic checkpoint store, journal outbound ops,
/// per-document delta supersede, verdicts on push. The store (GRDB, WAL) is
/// the app's raw truth; any native projection is rebuilt FROM it.
///
/// The core is codec-agnostic — `ReplicaManLoro` is the ONLY target that
/// imports Loro, so a rows-only consumer builds without the CRDT dependency
/// (enforced by the native target dependency graph).
let package = Package(
    name: "ReplicaMan",
    platforms: [.iOS(.v18), .macOS(.v15)],
    products: [
        .library(name: "ReplicaMan", targets: ["ReplicaMan"]),
        .library(name: "ReplicaManLoro", targets: ["ReplicaManLoro"]),
    ],
    dependencies: [
        // GRDB supplies WAL transactions and observations over the primary
        // local store. Public raw SQL access remains read-only.
        .package(url: "https://github.com/groue/GRDB.swift.git", from: "7.0.0"),
        // See docs/DEPENDENCIES.md for the verified binding/core matrix.
        // Common fixtures and real E2E establish the supported interoperability.
        .package(url: "https://github.com/loro-dev/loro-swift", exact: "1.13.3"),
        // Decode snapshot data directly into the strict ReplicaValue tree.
        .package(url: "https://github.com/ibireme/yyjson.git", from: "0.12.0"),
    ],
    targets: [
        .target(
            name: "ReplicaMan",
            dependencies: [
                .product(name: "GRDB", package: "GRDB.swift"),
                .product(name: "yyjson", package: "yyjson"),
            ]
        ),
        .target(
            name: "ReplicaManLoro",
            dependencies: [
                "ReplicaMan",
                .product(name: "Loro", package: "loro-swift"),
            ]
        ),
        .target(name: "ReplicaManTestProtocol", dependencies: ["ReplicaMan"]),
        .target(name: "ReplicaManGeneratedContract", dependencies: ["ReplicaMan"]),
        .executableTarget(
            name: "ReplicaManE2EWorker",
            dependencies: [
                "ReplicaMan",
                "ReplicaManLoro",
                .product(name: "Loro", package: "loro-swift"),
                .product(name: "GRDB", package: "GRDB.swift"),
            ]
        ),
        .executableTarget(name: "ReplicaManNotesExample", dependencies: ["ReplicaMan"]),
        // Core suite: NO loro import anywhere — building and running it is
        // the rows-only consumer proof.
        .testTarget(
            name: "ReplicaManTests",
            dependencies: ["ReplicaMan", "ReplicaManTestProtocol"],
            resources: [.copy("Fixtures/manifest.json")]
        ),
        // Codec + document-lifecycle suite: the only tests that touch Loro.
        .testTarget(
            name: "ReplicaManLoroTests",
            dependencies: [
                "ReplicaMan",
                "ReplicaManLoro",
                "ReplicaManTestProtocol",
                .product(name: "Loro", package: "loro-swift"),
            ],
            resources: [.copy("Fixtures/crdt_convergence")]
        ),
    ]
)
