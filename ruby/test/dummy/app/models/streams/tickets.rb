class Streams::Tickets < ReplicaMan::Stream
  key :code
  owner :user_id

  attribute :user_id, :note
  index :user_id
  index :note, kind: :fts5
end
