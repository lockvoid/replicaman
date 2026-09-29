import Foundation
import Testing
@testable import ReplicaManLoro
@testable import ReplicaMan

/// Undo has to be COLLABORATIVE: it inverts what THIS peer did and leaves
/// everyone else's work standing — on the document the engine holds, one step
/// per edit (the engine's undo merges nothing by time: a commit IS the
/// boundary of one user action).
///
/// A snapshot stack cannot express that — it can only reinstate a whole past
/// state, taking every concurrent edit down with it.
@Suite("Collaborative undo")
struct CollaborativeUndoTests {

    /// Us, and a peer who starts from our document and whose ops we import.
    private func pair(_ clips: [String: [String: ReplicaValue]]) throws -> (mine: HeldDocument, theirs: HeldDocument) {
        let mine = try HeldDocument(peer: 11)
        try mine.edit { try $0.writeRegistry("clips", clips, base: nil) }
        let theirs = try HeldDocument(peer: 22)
        try mine.sync(to: theirs)
        return (mine, theirs)
    }

    @Test("undo reverts our own edit")
    func undoRevertsOurEdit() throws {
        let (mine, _) = try pair(["a": clipFields(start: 0)])
        let base = mine.registry("clips")

        try mine.edit { try $0.writeRegistry("clips", ["a": clipFields(start: 5)], base: base) }
        #expect(mine.canUndo)

        #expect(try mine.undo())
        #expect(mine.entry("clips", "a")?["start"] == .number(0))
    }

    @Test("undo does not revert a peer's edit to another entry")
    func undoLeavesPeerEditAlone() throws {
        let (mine, theirs) = try pair(["a": clipFields(start: 0), "b": clipFields(start: 10)])
        let base = mine.registry("clips")

        // We move `a`.
        try mine.edit { try $0.writeRegistry("clips", ["a": clipFields(start: 5), "b": clipFields(start: 10)], base: base) }
        // Meanwhile they move `b`, and it reaches us.
        try theirs.edit { try $0.writeEntry("clips", "b", ["start": .number(42)], base: nil) }
        try theirs.sync(to: mine)

        #expect(try mine.undo())

        #expect(mine.entry("clips", "a")?["start"] == .number(0), "our edit must be reverted")
        #expect(mine.entry("clips", "b")?["start"] == .number(42), "the peer's edit must survive our undo")
    }

    @Test("undo does not delete an entry that appeared after our edit")
    func undoLeavesPeerInsertAlone() throws {
        let (mine, theirs) = try pair(["a": clipFields(start: 0)])
        let base = mine.registry("clips")

        try mine.edit { try $0.writeRegistry("clips", ["a": clipFields(start: 5)], base: base) }
        try theirs.edit {
            try $0.writeRegistry("clips", ["a": clipFields(start: 0), "b": clipFields(start: 10)], base: theirs.registry("clips"))
        }
        try theirs.sync(to: mine)

        #expect(try mine.undo())

        #expect(mine.entry("clips", "b") != nil, "undo deleted an entry it had never seen")
        #expect(mine.entry("clips", "a")?["start"] == .number(0))
    }

    @Test("undo does not resurrect an entry a peer deleted")
    func undoLeavesPeerDeleteAlone() throws {
        let (mine, theirs) = try pair(["a": clipFields(start: 0), "b": clipFields(start: 10)])
        let base = mine.registry("clips")

        try mine.edit { try $0.writeRegistry("clips", ["a": clipFields(start: 5), "b": clipFields(start: 10)], base: base) }
        try theirs.edit { try $0.delete(at: ["clips", "b"]) }
        try theirs.sync(to: mine)

        #expect(try mine.undo())

        #expect(mine.entry("clips", "b") == nil, "undo resurrected an entry the peer deleted")
        #expect(mine.entry("clips", "a")?["start"] == .number(0))
    }

    @Test("redo does not revert a peer's edit either")
    func redoLeavesPeerEditAlone() throws {
        let (mine, theirs) = try pair(["a": clipFields(start: 0), "b": clipFields(start: 10)])

        try mine.edit {
            try $0.writeRegistry("clips", ["a": clipFields(start: 5), "b": clipFields(start: 10)], base: mine.registry("clips"))
        }
        #expect(try mine.undo())

        try theirs.edit { try $0.writeEntry("clips", "b", ["start": .number(42)], base: nil) }
        try theirs.sync(to: mine)

        #expect(mine.canRedo)
        #expect(try mine.redo())

        #expect(mine.entry("clips", "a")?["start"] == .number(5), "redo must restore our edit")
        #expect(mine.entry("clips", "b")?["start"] == .number(42), "the peer's edit must survive our redo")
    }

    @Test("a peer's edit is not on OUR undo stack")
    func peerEditsAreNotOurs() throws {
        // The document opens from a fold another peer wrote: nothing in it is
        // ours to undo.
        let author = try HeldDocument(peer: 5)
        try author.edit { try $0.writeRegistry("clips", ["a": clipFields(start: 0)], base: nil) }
        let mine = try HeldDocument(peer: 11, fold: try author.snapshot())
        let theirs = try HeldDocument(peer: 22, fold: try author.snapshot())
        #expect(!mine.canUndo)

        try theirs.edit { try $0.writeEntry("clips", "a", ["start": .number(42)], base: nil) }
        try theirs.sync(to: mine)

        // Importing a peer's ops must not hand us the power to undo them.
        #expect(!mine.canUndo, "a peer's edit landed on our undo stack")
        #expect(mine.entry("clips", "a")?["start"] == .number(42))
    }

    @Test("our own edits undo one step at a time")
    func stepsAreDiscrete() throws {
        let document = try HeldDocument(peer: 11)
        try document.edit { try $0.writeRegistry("clips", ["a": clipFields(start: 0)], base: nil) }
        try document.edit { try $0.writeRegistry("clips", ["a": clipFields(start: 5)], base: document.registry("clips")) }
        try document.edit { try $0.writeRegistry("clips", ["a": clipFields(start: 9)], base: document.registry("clips")) }

        #expect(try document.undo())
        #expect(document.entry("clips", "a")?["start"] == .number(5))

        #expect(try document.undo())
        #expect(document.entry("clips", "a")?["start"] == .number(0))
    }

    @Test("undo reports false when there is nothing of ours to undo")
    func nothingToUndo() throws {
        let document = try HeldDocument(peer: 11)

        #expect(!document.canUndo)
        #expect(try !document.undo())
    }
}
