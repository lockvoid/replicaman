package io.replicaman

/** An opaque admission for local authoring into one bound owner world, including its generation. */
public class ReplicaLocalSession internal constructor(
    internal val engine: java.util.UUID,
    internal val binding: ULong,
)
