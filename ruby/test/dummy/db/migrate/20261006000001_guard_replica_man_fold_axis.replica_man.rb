class GuardReplicaManFoldAxis < ActiveRecord::Migration[8.1]
  def up
    execute <<~SQL
      CREATE OR REPLACE FUNCTION replica_man_refuse_unclaimed() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF EXISTS (SELECT 1 FROM replica_man_snapshots
                   WHERE namespace = NEW.namespace AND stream = NEW.stream AND row_id = NEW.row_id
                   AND (position = -1 OR document_position = -1)) THEN
          RAISE EXCEPTION 'replica_man: %/% committed without a claimed position', NEW.stream, NEW.row_id;
        END IF;
        RETURN NULL;
      END;
      $$;
      DROP TRIGGER replica_man_position_claimed ON replica_man_snapshots;
      CREATE CONSTRAINT TRIGGER replica_man_position_claimed
      AFTER INSERT OR UPDATE OF position, document_position ON replica_man_snapshots
      DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.position = -1 OR NEW.document_position = -1)
      EXECUTE FUNCTION replica_man_refuse_unclaimed();
    SQL
  end

  def down
    execute <<~SQL
      CREATE OR REPLACE FUNCTION replica_man_refuse_unclaimed() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF EXISTS (SELECT 1 FROM replica_man_snapshots
                   WHERE namespace = NEW.namespace AND stream = NEW.stream AND row_id = NEW.row_id AND position = -1) THEN
          RAISE EXCEPTION 'replica_man: %/% committed without a claimed position', NEW.stream, NEW.row_id;
        END IF;
        RETURN NULL;
      END;
      $$;
      DROP TRIGGER replica_man_position_claimed ON replica_man_snapshots;
      CREATE CONSTRAINT TRIGGER replica_man_position_claimed AFTER INSERT OR UPDATE OF position ON replica_man_snapshots
      DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.position = -1) EXECUTE FUNCTION replica_man_refuse_unclaimed();
    SQL
  end
end
