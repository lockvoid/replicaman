module ReplicaMan
  class Stream
    class Invalid < StandardError; end

    TYPE_SYMBOLS = %i[string integer float boolean json datetime date].freeze

    INDEX_KINDS = %i[btree fts5].freeze
    INDEXABLE_BTREE_TYPES = %w[string text integer bigint float decimal boolean datetime date].freeze
    INDEXABLE_FTS5_TYPES = %w[string text].freeze

    class << self
      attr_accessor :replica

      def stream_name
        name.demodulize.underscore
      end

      # The user whose bucket holds a row: a column name or a callable taking the
      # record. Rows of the `shared:` owner go to the bucket every principal reads;
      # a nil owner stops synchronizing the row.
      def owner(reader = nil, shared: nil)
        unless reader.nil?
          @owner = reader.respond_to?(:call) ? reader : ->(record) { record.public_send(reader) }
          @shared_owner = shared
        end
        @owner
      end

      def shared?
        !@shared_owner.nil?
      end

      def bucket_for(record)
        owner = self.owner.call(record)
        return if owner.nil?

        owner.to_s == @shared_owner&.call.to_s ? "#{shard}:*" : "#{shard}:#{owner}"
      end

      def own_bucket(user)
        "#{shard}:#{user.id}"
      end

      def door(klass = nil, variant: nil)
        unless klass.nil?
          @door = klass
          @door_variant = variant
        end
        @door
      end

      def door_variant
        @door_variant
      end

      def shard(value = nil)
        @shard = value.to_s unless value.nil?
        @shard || 'user'
      end

      def model(name = nil, prefix: nil)
        if name
          @model_name = name
          @table_prefix = prefix
          @model = nil
        end
        @model ||= @model_name ? @model_name.constantize : infer_model
      end

      def table_prefix
        @table_prefix
      end

      def key(column = nil)
        @key = column.to_s unless column.nil?
        @key || model.primary_key
      end

      # Recapture this stream when a computed pull reads changed parent fields.
      def depends_on(parent, via:, fields:)
        dependency = ProjectionDependencies::Dependency.new(parent, via: via, fields: fields)
        projection_dependencies << dependency
        Capture.install_flush(dependency.parent) if replica
      end

      def projection_dependencies
        @projection_dependencies ||= []
      end

      # A reference is sent with every mutation and checked under its parent's fence.
      def reference(name, stream:, field: nil, key_segment: nil, key_prefix: nil, optional: false)
        name = name.to_s.camelize(:lower)
        raise Invalid, "duplicate reference: #{name}" if references.any? { it.fetch(:name) == name }
        unless key_segment.nil? || (key_segment.is_a?(Integer) && key_segment >= 0)
          raise Invalid, 'reference key_segment must be a nonnegative integer'
        end
        raise Invalid, 'reference has both a field and a key segment' if field && key_segment
        raise Invalid, 'reference optional must be boolean' unless [true, false].include?(optional)
        spec = { name: name, stream: stream.to_s, optional: optional }
        if key_segment
          spec[:keySegment] = key_segment
          spec[:keyPrefix] = key_prefix if key_prefix
        else
          raise Invalid, 'key_prefix requires key_segment' if key_prefix
          spec[:field] = (field || name).to_s.camelize(:lower)
        end
        references << spec
      end

      def references
        @references ||= []
      end

      # Use only for derived rows whose lifetime belongs to their parent.
      # Recreating the parent changes the derived identity on every peer.
      def lifetime_from(reference = nil)
        if reference
          name = reference.to_s.camelize(:lower)
          spec = references.find { it.fetch(:name) == name }
          raise Invalid, "unknown lifetime reference: #{name}" unless spec
          raise Invalid, 'a lifetime reference cannot be optional' if spec.fetch(:optional)
          @lifetime_from = name
        end
        @lifetime_from
      end

      def attribute(*names, push: nil, pull: nil, intake: nil, precondition: false)
        if precondition && (@document_target || push == false || pull.respond_to?(:call) || intake)
          raise Invalid, "stream '#{stream_name}': a precondition rides every write of its row — declare it a plain pushed attribute"
        end
        return declare_document(names, push:, pull:, intake:) if @document_target

        unless push.nil? || push == true || push == false
          raise Invalid, "stream '#{stream_name}': push: is boolean only — inbound judgment is the door's monopoly"
        end
        if push == true && pull.respond_to?(:call) && intake.nil?
          raise Invalid, "stream '#{stream_name}': a computed pull attribute is never pushed — drop push: true"
        end
        if intake && !(push == true && (pull == false || pull.respond_to?(:call)))
          raise Invalid, "stream '#{stream_name}': an intake field is push-only — declare push: true with pull: false or a computed echo"
        end

        if names.size == 2 && type_declaration?(names.fetch(1))
          declare(names.fetch(0), type: names.fetch(1), push: push, pull: pull, intake: intake, precondition: precondition)
        else
          names.each { declare(it, type: nil, push: push, pull: pull, intake: intake, precondition: precondition) }
        end
      end

      def document(&block)
        @document_target = true
        class_eval(&block)
      ensure
        @document_target = nil
      end

      def index(*names, kind: :btree)
        unless INDEX_KINDS.include?(kind)
          raise Invalid, "stream '#{stream_name}' index kind :#{kind} — declare one of " \
                         "#{INDEX_KINDS.map(&:inspect).join(', ')}"
        end
        names.each do |name|
          index_declarations << { name: name.to_s, wire: name.to_s.camelize(:lower), kind: kind }
        end
      end

      def index_declarations
        @index_declarations ||= []
      end

      def indexes
        index_declarations
          .map { { field: it.fetch(:wire), kind: it.fetch(:kind).to_s } }
          .uniq
          .sort_by { [it.fetch(:field), it.fetch(:kind)] }
      end

      def preconditions
        base_declarations.values.select { it.fetch(:precondition) }.map { it.fetch(:wire) }.sort
      end

      def variant(klass, &block)
        variant_declarations[klass] ||= {}
        return unless block

        @variant_target = klass
        begin
          class_eval(&block)
        ensure
          @variant_target = nil
        end
      end

      def readonly
        door.nil?
      end

      def sti
        variant_declarations.any?
      end

      def normalizer
        @normalizer ||= (door || Normalizer::Row).new
      end

      def document?
        normalizer.is_a?(Normalizer::Document)
      end

      def document_declarations
        @document_declarations ||= {}
      end

      def reflected?(spec)
        document_declarations.key?(spec.fetch(:wire))
      end

      def reflected_paths
        base_declarations.values.select { reflected?(it) }.to_h { [it.fetch(:name), ['meta', it.fetch(:wire)]] }
      end

      def lane
        document? ? 'document' : 'row'
      end

      def document_schema?
        document_declarations.any?
      end

      def document_shapes
        shapes = document_roots.to_h do |spec|
          [spec.fetch(:wire), shape_type(raw_accessor_type(model, spec.fetch(:name)), path: "#{stream_name}.#{spec.fetch(:name)}", stack: [])]
        end
        scalars = document_scalars.map { it.fetch(:name) }
        shapes['meta'] = object_shape(model, path: "#{stream_name}.meta", stack: [], only: scalars) if scalars.any?
        shapes.sort.to_h
      end

      def document_seed
        record = model.new
        seed = document_roots.to_h { [it.fetch(:name), record.public_send(it.fetch(:name)).as_json] }.reject { |_, value| value == [] || value == {} }
        meta = document_scalars.to_h { [it.fetch(:name), record.public_send(it.fetch(:name))] }.compact
        seed['meta'] = meta if meta.any?
        seed
      end

      def document_default
        wire_value(document_seed)
      end

      def document_roots
        document_declarations.values.select { raw_accessor_type(model, it.fetch(:name)) }
      end

      def document_scalars
        document_declarations.values.reject { raw_accessor_type(model, it.fetch(:name)) }
      end

      def row_key(record)
        record.public_send(key).to_s
      end

      def locate(row_id, lock: false)
        relation = lock ? model.lock : model
        relation.find_by(key => row_id)
      end

      def wire_type(klass)
        klass.name.demodulize
      end

      def variant_class(type)
        variant_declarations.keys.find { wire_type(it) == type.to_s }
      end

      def validate!
        expected = model.table_name
        expected = expected.delete_prefix("#{table_prefix}_") if table_prefix
        unless expected == stream_name
          raise Invalid, "stream '#{stream_name}' must be named after its table '#{model.table_name}'" \
                         "#{" minus the declared '#{table_prefix}' prefix" if table_prefix} — one name, zero aliases"
        end

        if door.nil?
          flagged = (base_declarations.values + variant_declarations.values.flat_map(&:values))
            .select { it.fetch(:push_declared) || it.fetch(:pull) == false }
          if flagged.any?
            raise Invalid, "stream '#{stream_name}' has no door — no inbound exists, so " \
                           "#{flagged.map { ":#{it.fetch(:name)}" }.join(', ')} cannot declare push:/intake flags"
          end
        end
        if door_variant && !(door_variant < model)
          raise Invalid, "stream '#{stream_name}': door variant #{door_variant} is not a #{model} subclass"
        end
        variant_declarations.each_key do |klass|
          unless klass < model
            raise Invalid, "stream '#{stream_name}': variant #{klass} is not a #{model} subclass"
          end
        end
        if document_declarations.any? && !document?
          raise Invalid, "stream '#{stream_name}': a document block needs a document door"
        end
        if document? && document_declarations.empty?
          raise Invalid, "stream '#{stream_name}': a document door needs its document declared — document do … end"
        end
        raise Invalid, "stream '#{stream_name}' declares no owner" if owner.nil?
      end

      # The declarations against the migrated schema. The migration generator and the manifest run it;
      # loading the application never touches the database.
      def validate_schema!
        projection_dependencies.each { it.validate!(self) }
        validate_index_targets!
        validate_document!
        validate_declarations!(model, base_declarations)
        variant_declarations.each { |klass, declarations| validate_declarations!(klass, declarations) }
        validate_index_types!
      end

      def validate_document!
        document_declarations.each_value do |spec|
          next if raw_accessor_type(model, spec.fetch(:name)) || model.columns_hash.key?(spec.fetch(:name))

          raise Invalid, "stream '#{stream_name}' document attribute :#{spec.fetch(:name)} is not an attribute of #{model}"
        end
        base_declarations.each_value do |spec|
          next unless reflected?(spec) && raw_accessor_type(model, spec.fetch(:name))

          raise Invalid, "stream '#{stream_name}' attribute :#{spec.fetch(:name)}: a row reflects the document's scalars, " \
                         "and #{spec.fetch(:name)} is a document root"
        end
      end

      def validate_index_targets!
        index_declarations.each do |index|
          spec = base_declarations.fetch(index.fetch(:wire), nil)
          next if spec && spec.fetch(:pull) != false && !identity?(spec)

          raise Invalid, "stream '#{stream_name}' index :#{index.fetch(:name)} — an index is a derivative of a " \
                         'pulled base attribute; declare the attribute first'
        end
      end

      def validate_index_types!
        index_declarations.each do |index|
          type = manifest_entry(model, base_declarations.fetch(index.fetch(:wire))).fetch(:type)
          allowed = index.fetch(:kind) == :btree ? INDEXABLE_BTREE_TYPES : INDEXABLE_FTS5_TYPES
          next if allowed.include?(type)

          raise Invalid, "stream '#{stream_name}' index :#{index.fetch(:name)} (#{index.fetch(:kind)}) — a #{type} value " \
                         "cannot be indexed this way (#{allowed.join('/')})"
        end
      end

      def member!(user, record)
        bucket = record.is_a?(Snapshot) ? record.bucket : bucket_for(record)
        raise Refused, 'row is outside your replica' unless bucket == own_bucket(user)
      end

      def serialize(record)
        pull_declarations(record.class).each_with_object({}) do |spec, data|
          value =
            if spec.fetch(:pull).respond_to?(:call)
              spec.fetch(:pull).call(record)
            elsif spec.fetch(:store_key)
              record.public_send(spec.fetch(:name))
            else
              record.read_attribute(spec.fetch(:name))
            end

          data[spec.fetch(:wire)] = spec.fetch(:shape) ? translate_shape(value, spec.fetch(:shape)) : value
        end
      end

      def intake_names
        (base_declarations.values + variant_declarations.values.flat_map(&:values))
          .select { it.fetch(:pull) == false || it.fetch(:intake) }.map { it.fetch(:name) }
      end

      def intake_specs
        (base_declarations.values + variant_declarations.values.flat_map(&:values))
          .select { it.fetch(:intake) }
      end

      def decode(klass, data)
        specs = declaration_index(klass)

        data.to_h.each_with_object({}) do |(wire_key, value), attributes|
          spec = specs.fetch(wire_key.to_s, nil)
          raise Refused, "unknown column: #{wire_key}" if spec.nil? || identity?(spec)
          next if spec.fetch(:pull).respond_to?(:call) && !spec.fetch(:intake)
          raise Refused, "unknown column: #{wire_key}" unless push?(spec)

          attributes[spec.fetch(:name)] = spec.fetch(:shape) ? translate_shape(value, spec.fetch(:shape), inbound: true) : value
        end
      end

      def wire_columns
        base_declarations.values
          .reject { identity?(it) }
          .map { manifest_entry(model, it) }
          .sort_by { it.fetch(:name) }
      end

      def variants
        variant_declarations.map do |klass, declarations|
          {
            type: wire_type(klass),
            columns: declarations.values.map { manifest_entry(klass, it, variant: true) }.sort_by { it.fetch(:name) },
          }
        end.sort_by { it.fetch(:type) }
      end

      def introspectable?
        model.table_exists? && !model.connection_pool.migration_context.needs_migration?
      rescue ActiveRecord::NoDatabaseError, ActiveRecord::ConnectionNotDefined
        # Declarations also load before db:create or database configuration.
        # Validation runs once configured; connection failures still propagate.
        false
      end

      private

      def base_declarations
        @base_declarations ||= {}
      end

      def variant_declarations
        @variant_declarations ||= {}
      end

      def type_declaration?(value)
        return true unless value.is_a?(Symbol)

        TYPE_SYMBOLS.include?(value)
      end

      def declare_document(names, push:, pull:, intake:)
        unless push.nil? && pull.nil? && intake.nil?
          raise Invalid, "stream '#{stream_name}': a document attribute travels in the document's deltas — " \
                         'it takes no push:, pull: or intake:'
        end

        names.each do |name|
          unless name.is_a?(Symbol) && !TYPE_SYMBOLS.include?(name)
            raise Invalid, "stream '#{stream_name}': a document attribute names a model attribute — its type is introspected"
          end

          document_declarations[name.to_s.camelize(:lower)] = { name: name.to_s, wire: name.to_s.camelize(:lower) }
        end
      end

      def declare(name, type:, push:, pull:, intake: nil, precondition: false)
        spec = {
          name: name.to_s,
          wire: name.to_s.camelize(:lower),
          type: type,
          push: push,
          push_declared: !push.nil?,
          pull: pull.nil? ? true : pull,
          intake: intake,
          precondition: precondition,
        }

        if @variant_target
          variant_declarations.fetch(@variant_target)[spec.fetch(:wire)] = spec
        else
          base_declarations[spec.fetch(:wire)] = spec
        end
      end

      def push?(spec)
        return false if door.nil?
        return false if reflected?(spec)
        return false if spec.fetch(:pull).respond_to?(:call) && !spec.fetch(:intake)

        spec.fetch(:push).nil? ? true : spec.fetch(:push)
      end

      def identity?(spec)
        spec.fetch(:name) == key || spec.fetch(:name) == model.primary_key
      end

      def validate_declarations!(klass, declarations)
        declarations.each_value do |spec|
          if identity?(spec)
            if spec.fetch(:type) || spec.fetch(:push_declared) || spec.fetch(:pull) != true
              raise Invalid, "stream '#{stream_name}' attribute :#{spec.fetch(:name)}: " \
                             'the identity rides the op envelope — it takes no type and no directions'
            end
            next
          end
          if spec.fetch(:name) == klass.inheritance_column && klass.columns_hash.key?(spec.fetch(:name))
            raise Invalid, "stream '#{stream_name}' attribute :#{spec.fetch(:name)}: " \
                           'the STI discriminator rides op.type, never the data'
          end

          column = store_columns(klass).include?(spec.fetch(:name)) ? nil : klass.columns_hash.fetch(spec.fetch(:name), nil)
          if column
            validate_column_type!(spec, column)
          elsif accessor_types(klass).key?(spec.fetch(:name).to_sym) || accessor_types(klass).key?(spec.fetch(:name))
            if spec.fetch(:type)
              raise Invalid, "stream '#{stream_name}' attribute :#{spec.fetch(:name)}: " \
                             'a typed store key is introspected — remove the declared type'
            end
          else
            validate_columnless!(spec)
          end
        end
      end

      def validate_column_type!(spec, column)
        if column.type.in?(%i[json jsonb])
          if spec.fetch(:type).is_a?(Symbol)
            raise Invalid, "stream '#{stream_name}' attribute :#{spec.fetch(:name)}: " \
                           'a json column declares a shape source (a StoreModel class or ReplicaMan.union), not a scalar'
          end
        elsif spec.fetch(:type)
          raise Invalid, "stream '#{stream_name}' attribute :#{spec.fetch(:name)}: " \
                         'the column type is introspected — remove the declared type'
        end
      end

      def validate_columnless!(spec)
        if spec.fetch(:type).nil?
          raise Invalid, "stream '#{stream_name}' declares unknown attribute: #{spec.fetch(:name)}"
        end
        unless spec.fetch(:pull) == false || spec.fetch(:pull).respond_to?(:call)
          raise Invalid, "stream '#{stream_name}' attribute :#{spec.fetch(:name)} has no column — " \
                         'declare pull: false (intake) or a pull lambda (computed)'
        end
      end

      def infer_model
        segments = stream_name.split('_')
        (0...segments.size).each do |split|
          candidate =
            if split.zero?
              stream_name.classify
            else
              "#{segments[0...split].join('_').camelize}::#{segments[split..].join('_').classify}"
            end
          # Missing candidates are expected during inference. safe_constantize
          # still raises errors from inside a model's own class definition.
          resolved = candidate.safe_constantize
          return resolved if resolved
        end

        raise Invalid, "stream '#{stream_name}' resolves no model — declare one (`model 'Some::Model'`)"
      end

      def pull_declarations(klass)
        resolved_declarations(klass).values.reject { it.fetch(:pull) == false || identity?(it) }
      end

      def declaration_index(klass)
        resolved_declarations(klass)
      end

      def resolved_declarations(klass)
        @resolved ||= {}
        @resolved[klass] ||= begin
          specs = base_declarations.dup
          variant_declarations.each do |variant_klass, declarations|
            specs = specs.merge(declarations) if klass <= variant_klass
          end
          specs.transform_values do |spec|
            spec.merge(store_key: accessor_types(klass).key?(spec.fetch(:name).to_sym) ||
                                  accessor_types(klass).key?(spec.fetch(:name)),
                       shape: manifest_entry(klass, spec).fetch(:shapes, nil))
          end
        end
      end

      def manifest_entry(klass, spec, variant: false)
        entry =
          if store_columns(klass).exclude?(spec.fetch(:name)) && (column = klass.columns_hash.fetch(spec.fetch(:name), nil))
            declared = column_type(klass, spec.fetch(:name))
            base = { name: spec.fetch(:wire), type: declared.fetch(:type) }
            base[:null] = variant ? !required_shape_field?(klass, spec.fetch(:name)) : column.null
            base[:items] = declared.fetch(:items, nil) if declared.fetch(:items, nil)
            base
          elsif (raw = raw_accessor_type(klass, spec.fetch(:name)))
            declared = store_key_type(raw, spec)
            base = { name: spec.fetch(:wire), type: declared.fetch(:type) }
            base[:null] = !required_shape_field?(klass, spec.fetch(:name))
            base[:items] = declared.fetch(:items, nil) if declared.fetch(:items, nil)
            base[:shapes] = store_key_shape(raw, spec) if raw.respond_to?(:model_klass)
            base
          else
            { name: spec.fetch(:wire), type: columnless_manifest_type(spec), null: true }
          end

        enum = enum_values(klass, spec.fetch(:name), entry.fetch(:type))
        entry[:enum] = enum if enum
        attach_shape(entry, spec)
        entry[:push] = push?(spec)
        entry[:pull] = spec.fetch(:pull) != false
        entry[:precondition] = true if spec.fetch(:precondition)
        entry[:reflects] = ['meta', spec.fetch(:wire)] if !variant && reflected?(spec)
        entry
      end

      def columnless_manifest_type(spec)
        spec.fetch(:type).is_a?(Symbol) ? spec.fetch(:type).to_s : 'json'
      end

      def raw_accessor_type(klass, name)
        accessor_types(klass).fetch(name.to_sym) { accessor_types(klass).fetch(name, nil) }
      end

      def store_key_shape(raw, spec)
        object = object_shape(raw.model_klass, path: "#{stream_name}.#{spec.fetch(:name)}", stack: [], only: nil)
        raw.type == :array ? { type: 'array', items: object } : object
      end

      def attach_shape(entry, spec)
        source = spec.fetch(:type)
        return if source.nil? || source.is_a?(Symbol)

        entry[:shapes] =
          if source.is_a?(Union)
            union_shape(source)
          else
            declared_shape(source, path: "#{stream_name}.#{spec.fetch(:name)}", stack: [])
          end
      end

      def union_shape(union)
        variants = union.variants.map do |value, klass|
          model_name = klass.name&.demodulize
          unless model_name.present? && klass.respond_to?(:attribute_types)
            raise ArgumentError, "union variant #{value.inspect} for #{stream_name} must be a named StoreModel class"
          end

          {
            value: value.to_s,
            type: model_name,
            fields: shape_fields(
              klass,
              path: "#{stream_name}.#{value}",
              stack: [],
              omit: [union.discriminator.underscore]
            ),
          }
        end.sort_by { it.fetch(:value) }

        { discriminator: union.discriminator.camelize(:lower), variants: variants }
      end

      def column_type(klass, attribute)
        type = klass.attribute_types.fetch(attribute)
        return { type: type.type.to_s } unless pg_array?(type)

        shape_type(type, path: "#{stream_name}.#{attribute}", stack: [])
      end

      def store_key_type(raw, spec)
        return { type: raw.type.to_s } unless raw.type == :array && raw.respond_to?(:subtype)

        shape_type(raw, path: "#{stream_name}.#{spec.fetch(:name)}", stack: [])
      end

      def pg_array?(type)
        type.class.name == 'ActiveRecord::ConnectionAdapters::PostgreSQL::OID::Array'
      end

      def enum_values(klass, attribute, type)
        return unless type == 'string'

        sets = klass.validators_on(attribute.to_sym).filter_map do |validator|
          next unless validator.is_a?(ActiveModel::Validations::InclusionValidator)
          next if conditional_validator?(validator)

          source = validator.options.fetch(:in, nil) || validator.options.fetch(:within, nil)
          next if contains_proc?(source) || !source.respond_to?(:to_a)

          source.to_a.map(&:to_s)
        end
        return if sets.empty?

        sets.drop(1).reduce(sets.first.uniq) { |values, allowed| values & allowed }
      end

      def conditional_validator?(validator)
        validator.options.key?(:if) || validator.options.key?(:unless) || contains_proc?(validator.options)
      end

      def contains_proc?(value)
        case value
        when Proc
          true
        when Array
          value.any? { contains_proc?(it) }
        when Hash
          value.any? { contains_proc?(it) }
        else
          false
        end
      end

      def shape_fields(klass, path:, stack:, omit: [], only: nil)
        if stack.include?(klass)
          raise ArgumentError, "recursive StoreModel shape cannot be represented at #{path}"
        end

        next_stack = stack + [klass]
        klass.attribute_types.filter_map do |attribute, type|
          next if omit.include?(attribute.to_s)
          next if only && only.exclude?(attribute.to_s)

          shape = shape_type(type, path: "#{path}.#{attribute}", stack: next_stack).dup
          wire_type = shape.delete(:type)
          entry = {
            name: attribute.to_s.camelize(:lower),
            type: wire_type,
            null: !required_shape_field?(klass, attribute),
          }.merge(shape)

          if wire_type == 'array' && entry.dig(:items, :type) == 'string'
            enum = enum_values(klass, attribute, 'string')
            entry.fetch(:items)[:enum] = enum if enum
          else
            enum = enum_values(klass, attribute, wire_type)
            entry[:enum] = enum if enum
          end
          default = shape_field_default(klass, attribute)
          entry[:default] = wire_value(default) unless default.equal?(UNSET_DEFAULT)
          entry
        end.sort_by { it.fetch(:name) }
      end

      UNSET_DEFAULT = Object.new.freeze

      def shape_field_default(klass, attribute)
        defaults = klass.send(:_default_attributes)
        value = defaults[attribute.to_s]
        return UNSET_DEFAULT unless value.is_a?(ActiveModel::Attribute::UserProvidedDefault)

        source = value.send(:user_provided_value)
        return UNSET_DEFAULT if source.is_a?(Proc) && source.call != source.call

        value.value
      end

      def declared_shape(declaration, path:, stack:)
        declaration = resolve(declaration)
        if declaration.is_a?(Hash) && (declaration.key?(:model) || declaration.key?('model'))
          model = resolve(declaration.fetch(:model) { declaration.fetch('model') })
          only = declaration.fetch(:only) { declaration.fetch('only', nil) }
          collection = declaration.fetch(:collection) { declaration.fetch('collection', nil) }
          object = object_shape(model, path:, stack:, only:)
          return { type: 'array', items: object } if collection.to_s == 'array'
          return object if collection.blank? || collection.to_s == 'object'

          raise ArgumentError, "unsupported shape collection #{collection.inspect} at #{path}"
        end

        return object_shape(declaration, path:, stack:) if declaration.respond_to?(:attribute_types)

        shape_type(declaration, path:, stack:)
      end

      def object_shape(klass, path:, stack:, only: nil)
        model_name = klass.name&.demodulize
        unless model_name.present? && klass.respond_to?(:attribute_types)
          raise ArgumentError, "shape source at #{path} must be a named model with attribute types"
        end

        {
          type: 'object',
          fields: shape_fields(klass, path:, stack:, only: Array(only).map(&:to_s).presence),
        }
      end

      def required_shape_field?(klass, attribute)
        klass.validators_on(attribute.to_sym).any? do |validator|
          next false if conditional_validator?(validator)
          next false if validator.options.fetch(:allow_nil, false) || validator.options.fetch(:allow_blank, false)

          validator.is_a?(ActiveModel::Validations::PresenceValidator) ||
            (validator.is_a?(ActiveModel::Validations::ExclusionValidator) &&
             validator.options.fetch(:in, nil) == [nil])
        end
      end

      def shape_type(type, path:, stack:)
        nested = type.model_klass if type.respond_to?(:model_klass)
        if nested
          object = { type: 'object', fields: shape_fields(nested, path: path, stack: stack) }
          return { type: 'array', items: object } if type.type.to_s == 'array'
          return object unless type.type.to_s == 'hash'

          return { type: 'map', values: object }
        end

        if type.respond_to?(:subtype)
          items = shape_type(type.subtype, path: "#{path}[]", stack: stack)
          items = items.merge(null: true) if type.try(:null)
          return { type: 'array', items: items }
        end

        wire_type = type.type&.to_s
        if wire_type.blank? || wire_type.in?(%w[array polymorphic polymorphic_array])
          raise ArgumentError, "StoreModel type is not introspectable at #{path} (#{type.class.name})"
        end

        { type: wire_type }
      end

      def resolve(value)
        value.respond_to?(:call) ? value.call : value
      end

      def translate_shape(value, shape, inbound: false)
        value = plain(value)

        if shape.fetch(:discriminator, nil) && value.is_a?(Hash)
          discriminator = shape.fetch(:discriminator)
          kind = value.fetch(discriminator) { value.fetch(discriminator.underscore, nil) }
          variant = shape.fetch(:variants).find { it.fetch(:value) == kind }
          return value unless variant

          fields = [{ name: discriminator, type: 'string' }] + variant.fetch(:fields)
          return translate_fields(value, fields, inbound: inbound)
        end

        case shape.fetch(:type, nil)
        when 'object'
          value.is_a?(Hash) ? translate_fields(value, shape.fetch(:fields), inbound: inbound) : value
        when 'array'
          value.is_a?(Array) ? value.map { translate_shape(it, shape.fetch(:items), inbound: inbound) } : value
        when 'map'
          value.is_a?(Hash) ? value.transform_values { translate_shape(it, shape.fetch(:values), inbound: inbound) } : value
        else
          value
        end
      end

      def plain(value)
        return value if value.is_a?(Hash) || value.is_a?(Array)

        held_json(value)
      end

      def held_json(value)
        json = value.as_json
        return json unless json.is_a?(Hash) && value.respond_to?(:attributes)

        value.attributes.each do |name, held|
          next unless json.key?(name)

          case held
          when Hash
            json[name] = held.as_json if json.fetch(name).is_a?(String)
          when Array
            json[name] = held.map { held_json(it) }
          else
            json[name] = held_json(held) if held.respond_to?(:attributes)
          end
        end
        json
      end

      def translate_fields(value, fields, inbound:)
        fields.each_with_object(value.stringify_keys) do |field, translated|
          wire_name = field.fetch(:name)
          model_name = wire_name.underscore
          source, destination = inbound ? [wire_name, model_name] : [model_name, wire_name]
          source = destination unless translated.key?(source)
          next unless translated.key?(source)

          translated[destination] = translate_shape(translated.delete(source), field, inbound: inbound)
        end
      end

      def wire_value(value)
        value = value.as_json if value.respond_to?(:as_json) && !value.is_a?(Hash) && !value.is_a?(Array)

        case value
        when Hash
          value.to_h do |key, nested|
            [key.to_s.camelize(:lower), wire_value(nested)]
          end.sort.to_h
        when Array
          value.map { wire_value(it) }
        else
          value
        end
      end

      def store_columns(klass)
        klass.attribute_types.filter_map { |column_name, type| column_name if typed_store?(type) }
      end

      def accessor_types(klass)
        klass.attribute_types.values.select { typed_store?(it) }
             .flat_map { it.send(:accessor_types).to_a }.to_h
      end

      def typed_store?(type)
        type.class.name == 'ActiveRecord::Type::TypedStore'
      end
    end
  end
end
