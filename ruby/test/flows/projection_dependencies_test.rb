require 'test_helper'

class ProjectionDependenciesTest < ActiveSupport::TestCase
  setup do
    @declarations = Streams::Jobs.send(:base_declarations).deep_dup
    @dependencies = Streams::Jobs.projection_dependencies.dup
    Streams::Jobs.attribute :owner_name, :string, pull: ->(job) { job.user.name }
    Streams::Jobs.depends_on User, via: :user_id, fields: [:name]
    Streams::Jobs.instance_variable_set(:@resolved, nil)
    migrate_streams!
    @user = User.create!(id: 'owner', name: 'Before')
    @job = Job.create!(id: 'job', user: @user, state: 'queued')
  end

  teardown do
    Streams::Jobs.instance_variable_set(:@base_declarations, @declarations)
    Streams::Jobs.instance_variable_set(:@resolved, nil)
    Streams::Jobs.instance_variable_set(:@projection_dependencies, @dependencies)
    migrate_streams!
  end

  def snapshot(id = @job.id)
    ReplicaMan::Snapshot.find_by!(namespace: DummyReplica.namespace, stream: 'jobs', row_id: id)
  end

  test 'a parent save recaptures computed children in the same commit' do
    revision = snapshot.revision
    @user.update!(name: 'After')
    assert_equal 'After', snapshot.data.fetch('ownerName')
    assert_operator snapshot.revision, :>, revision
    assert_empty ReplicaMan::CaptureHooks.changes(ActiveRecord::Base.connection)
  end

  test 'bulk parent updates recapture only affected children and actual field changes' do
    other = User.create!(id: 'other', name: 'Other')
    Job.create!(id: 'other-job', user: other, state: 'queued')
    other_revision = snapshot('other-job').revision

    DummyReplica.transaction { User.where(id: @user.id).update_all(name: 'Bulk') }
    assert_equal 'Bulk', snapshot.data.fetch('ownerName')
    assert_equal other_revision, snapshot('other-job').revision

    revision = snapshot.revision
    DummyReplica.transaction { User.where(id: @user.id).update_all(name: 'Bulk') }
    assert_equal revision, snapshot.revision
  end

  test 'an uncaptured bulk parent update fails before its transaction commits' do
    assert_raises(ActiveRecord::StatementInvalid) do
      User.where(id: @user.id).update_all(name: 'Must roll back')
    end
    assert_equal 'Before', @user.reload.name
    assert_equal 'Before', snapshot.data.fetch('ownerName')
  end

  test 'a rolled back parent write cannot leak into a later child capture' do
    DummyReplica.transaction do
      ActiveRecord::Base.transaction(requires_new: true) do
        @user.update!(name: 'Rolled back')
        raise ActiveRecord::Rollback
      end
      @job.update!(state: 'running')
    end
    assert_equal 'Before', @user.reload.name
    assert_equal 'Before', snapshot.data.fetch('ownerName')
    assert_equal 'running', snapshot.data.fetch('state')
  end

  test 'removing a declaration removes its database trigger' do
    Streams::Jobs.instance_variable_set(:@projection_dependencies, @dependencies)
    migrate_streams!
    revision = snapshot.revision
    @user.update!(name: 'No longer a dependency')
    assert_equal revision, snapshot.revision
  end

  test 'unknown dependency columns fail configuration validation' do
    dependency = ReplicaMan::ProjectionDependencies::Dependency.new(User, via: :user_id, fields: [:typo])
    assert_raises(ReplicaMan::Stream::Invalid) { dependency.validate!(Streams::Jobs) }
  end
end
