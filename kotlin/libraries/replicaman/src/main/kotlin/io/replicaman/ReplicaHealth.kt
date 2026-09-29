package io.replicaman

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import java.time.Instant

public enum class ReplicaFailureKind { TRANSPORT, STORAGE, UPGRADE_REQUIRED, RECOVERY_REQUIRED, OTHER }

public data class ReplicaFailure(
    val operation: String, val message: String, val occurredAt: Instant, val error: Exception,
) {
    public val kind: ReplicaFailureKind get() = when (error) {
        is ReplicaError.Transport -> ReplicaFailureKind.TRANSPORT
        is ReplicaError.Storage -> ReplicaFailureKind.STORAGE
        is ReplicaError.Protocol -> when (error.code) {
            "UpgradeRequired" -> ReplicaFailureKind.UPGRADE_REQUIRED
            "DatasetChanged", "NamespaceChanged", "MutationChanged" -> ReplicaFailureKind.RECOVERY_REQUIRED
            else -> ReplicaFailureKind.OTHER
        }
        else -> ReplicaFailureKind.OTHER
    }
}

/** Observable failures from work without an awaiting caller. */
public class ReplicaHealth {
    private val mutableFailure = MutableStateFlow<ReplicaFailure?>(null)
    public val failure: StateFlow<ReplicaFailure?> = mutableFailure.asStateFlow()

    /** Report a host background failure that has no awaiting caller. */
    public fun record(error: Exception, operation: String) {
        if (error is CancellationException) throw error
        mutableFailure.value = ReplicaFailure(operation, error.toString(), Instant.now(), error)
        Log.logger.error("[health] $operation: $error")
    }
}
