# frozen_string_literal: true

module ReplicaMan
  module Loro
    # Schema-independent, base-relative document writes. This module does not
    # commit. The caller owns the transaction and must discard the document on
    # any error; a refused entry must never become a successful partial save.
    module Writer
      module_function

      # entries/base are key => field-map. A nil base is authoritative; an
      # explicit base preserves unseen additions and a peer's later deletions.
      def write_registry(doc, root, entries, base: nil)
        registry = doc.get_map(root)
        live = registry.keys
        ((base&.keys || live) - entries.keys).each { registry.delete(it) }
        entries.each do |key, fields|
          next if base&.key?(key) && !live.include?(key)

          write_fields(registry.ensure_mergeable_map(key), fields, base: base&.dig(key))
        end
      end

      def write_fields(map, fields, base: nil)
        fields.each do |field, value|
          next if base&.key?(field.to_s) && base.fetch(field.to_s) == value

          write_field(map, field, value)
        end
      end

      def write_field(map, field, value)
        field = field.to_s
        return if value.nil? && !map.key?(field)
        return if map.get(field) == value

        map.set(field, value)
      end
    end
  end
end
