import Foundation
import XCTest
@testable import ReplicaMan

/// The host decides where the worlds live: `ReplicaMan.Configuration.homePath`
/// is the engine's home unless a caller hands it one.
final class ConfigurationTests: XCTestCase {

    func testAnEngineOpensUnderTheConfiguredHome() async throws {
        let home = Fixture.directory()
        ReplicaMan.Configuration.homePath = home
        let engine = ReplicaEngine(transport: StubTransport(), schema: Fixture.schema(), codecs: [StubCodec()])

        try await engine.open(owner: 7)

        XCTAssertEqual(engine.storePath?.path, home.appendingPathComponent("replica-7.sqlite").path)
        XCTAssertTrue(FileManager.default.fileExists(atPath: home.appendingPathComponent("replica-7.sqlite").path))
    }

    func testAnExplicitHomeWins() async throws {
        ReplicaMan.Configuration.homePath = Fixture.directory("configured")
        let home = Fixture.directory()
        let engine = Fixture.unopenedEngine(in: home, transport: StubTransport())

        try engine.openForColdBoot(owner: 7)

        XCTAssertEqual(engine.storePath?.path, home.appendingPathComponent("replica-7.sqlite").path)
    }
}
