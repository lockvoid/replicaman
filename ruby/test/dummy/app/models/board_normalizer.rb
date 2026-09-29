class BoardNormalizer < ReplicaMan::Normalizer::Document
  QUOTA = 2

  attr_accessor :after_create

  def create(replica, stream, op)
    super
    after_create&.call(replica, op)
    nil
  end

  def refuse?(op)
    return false unless op.verb == 'row.create'

    'board quota reached' if Board.where(user_id: op.user.id).count >= QUOTA
  end

  def project(doc, op: nil)
    op ? { user_id: op.user.id } : {}
  end
end
