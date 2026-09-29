import Foundation
import Darwin

/// An OS lock survives neither process death nor close. The lock file is never
/// unlinked: replacing its inode while another process holds it defeats exclusion.
final class ReplicaStoreLease: @unchecked Sendable {
    private let mutex = NSLock()
    private var descriptor: Int32

    init(path: String) throws {
        let url = URL(fileURLWithPath: path).resolvingSymlinksInPath()
        descriptor = Darwin.open(url.path + ".author.lock", O_RDWR | O_CREAT | O_CLOEXEC, S_IRUSR | S_IWUSR)
        guard descriptor >= 0 else { throw ReplicaError.storage("Cannot open authoring lease: \(errno)") }
        guard flock(descriptor, LOCK_EX | LOCK_NB) == 0 else {
            let lockError = errno
            let closeResult = Darwin.close(descriptor)
            descriptor = -1
            throw ReplicaError.storage("Cannot acquire store lease (errno \(lockError), close result \(closeResult))")
        }
    }

    func release() throws {
        mutex.lock()
        defer { mutex.unlock() }
        if descriptor >= 0 {
            let closing = descriptor
            descriptor = -1
            guard Darwin.close(closing) == 0 else {
                throw ReplicaError.storage("Cannot close authoring lease: \(errno)")
            }
        }
    }

    deinit {
        do {
            try release()
        } catch {
            // Destructors cannot throw. Explicit store.close() reports failures
            // to its caller; this final fallback only releases an abandoned lease.
            Log.logger.error("[lease] cleanup failed: \(String(describing: error), privacy: .public)")
        }
    }
}
