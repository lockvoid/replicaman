class CreateSampleStreams < ActiveRecord::Migration[8.1]
  def change
    create_table :decks, id: :string do |t|
      t.bigint :user_id, null: false
      t.integer :change_seq, null: false, default: 0
      t.jsonb :content, null: false, default: {}
      t.timestamps
    end

    create_table :item_templates, id: :string do |t|
      t.string :user_id, null: false
      t.string :type, null: false
      t.string :name, null: false
      t.jsonb :metadata, null: false, default: {}
    end

    create_table :themes, id: :string do |t|
      t.bigint :user_id, null: false
      t.string :name, null: false
      t.text :description
      t.jsonb :colors, null: false, default: []
      t.jsonb :heading
      t.boolean :pinned
      t.string :logo_ref
      t.timestamps
    end

    create_table :exports, id: :string do |t|
      t.string :user_id, null: false
      t.float :progress, null: false, default: 0
      t.jsonb :result, null: false, default: {}
      t.float :markers, array: true, null: false, default: []
      t.jsonb :details
    end

    create_table :workflows, id: :string do |t|
      t.integer :version, null: false, default: 1
      t.jsonb :graph, null: false, default: {}
    end
  end
end
