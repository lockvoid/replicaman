package io.replicaman

import java.io.File
import java.io.RandomAccessFile
import java.nio.channels.FileLock

/** Never delete the lock file; replacing its inode defeats exclusion. */
internal class ReplicaStoreLease(path: File) : AutoCloseable {
    private val file: RandomAccessFile
    private val lock: FileLock

    init {
        path.parentFile?.let { java.nio.file.Files.createDirectories(it.toPath()) }
        file = RandomAccessFile(path.canonicalPath + ".author.lock", "rw")
        try {
            lock = file.channel.tryLock() ?: throw ReplicaError.Storage("Store already has an authoring engine")
        } catch (error: Throwable) {
            try {
                file.close()
            } catch (closing: Throwable) {
                error.addSuppressed(closing)
            }
            throw error
        }
    }

    @Synchronized override fun close() {
        if (lock.isValid) lock.release()
        file.close()
    }
}
