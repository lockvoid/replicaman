# frozen_string_literal: true

require "fileutils"
require "json"
require "loro"

ROOT = File.expand_path("convergence", __dir__)
FileUtils.rm_rf(ROOT)
FileUtils.mkdir_p(ROOT)

def write_case(name, manifest:, blobs:)
  directory = File.join(ROOT, name)
  FileUtils.rm_rf(directory)
  FileUtils.mkdir_p(directory)

  blobs.each { |filename, bytes| File.binwrite(File.join(directory, filename), bytes) }

  converged = Loro::Doc.new
  status = converged.import_batch(blobs.values.reverse)
  raise "fixture #{name} remained pending" if status.fetch(:pending)

  File.write(File.join(directory, "manifest.json"), JSON.pretty_generate(manifest) + "\n")
  File.write(File.join(directory, "expected.json"), JSON.pretty_generate(converged.to_h) + "\n")
end

def independent_actor(peer_id, operations)
  doc = Loro::Doc.new(peer_id: peer_id)
  operations.each do |map_name, action, key, value|
    map = doc.get_map(map_name)
    action == :delete ? map.delete(key) : map.set(key, value)
  end
  doc.export_updates
end

write_case(
  "concurrent_map_values",
  manifest: {
    "peers" => [1, 2],
    "description" => "Independent peers preserve distinct keys and resolve one competing map key.",
    "operations" => {
      "1" => [["document", "set", "left", 1], ["document", "set", "title", "alpha"]],
      "2" => [["document", "set", "right", 2], ["document", "set", "title", "beta"]]
    }
  },
  blobs: {
    "01_peer_1.update.bin" => independent_actor(
      1,
      [["document", :set, "left", 1], ["document", :set, "title", "alpha"]]
    ),
    "02_peer_2.update.bin" => independent_actor(
      2,
      [["document", :set, "right", 2], ["document", :set, "title", "beta"]]
    )
  }
)

nested_base = Loro::Doc.new(peer_id: 10)
nested_base.get_map("root").ensure_mergeable_map("child")
nested_common = nested_base.version_vector
nested_base_blob = nested_base.export_updates
nested_snapshot = nested_base.export_snapshot
nested_left = Loro::Doc.from_snapshot(nested_snapshot, peer_id: 11)
nested_right = Loro::Doc.from_snapshot(nested_snapshot, peer_id: 12)
nested_left.get_map("root").get_map("child").set("left", 1)
nested_right.get_map("root").get_map("child").set("right", 2)
write_case(
  "concurrent_nested_map",
  manifest: {
    "peers" => [10, 11, 12],
    "description" => "Two peers edit different keys in the same pre-existing nested map.",
    "operations" => {
      "10" => [["root", "ensure_mergeable_map", "child"]],
      "11" => [["root.child", "set", "left", 1]],
      "12" => [["root.child", "set", "right", 2]]
    }
  },
  blobs: {
    "01_base_peer_10.update.bin" => nested_base_blob,
    "02_left_peer_11.update.bin" => nested_left.export_updates(since: nested_common),
    "03_right_peer_12.update.bin" => nested_right.export_updates(since: nested_common)
  }
)

delete_base = Loro::Doc.new(peer_id: 20)
delete_base.get_map("document").set("status", "base")
delete_common = delete_base.version_vector
delete_base_blob = delete_base.export_updates
delete_snapshot = delete_base.export_snapshot
deleting = Loro::Doc.from_snapshot(delete_snapshot, peer_id: 21)
setting = Loro::Doc.from_snapshot(delete_snapshot, peer_id: 22)
deleting.get_map("document").delete("status")
setting.get_map("document").set("status", "updated")
write_case(
  "delete_vs_set",
  manifest: {
    "peers" => [20, 21, 22],
    "description" => "A concurrent delete and replacement of the same map key resolve deterministically.",
    "operations" => {
      "20" => [["document", "set", "status", "base"]],
      "21" => [["document", "delete", "status"]],
      "22" => [["document", "set", "status", "updated"]]
    }
  },
  blobs: {
    "01_base_peer_20.update.bin" => delete_base_blob,
    "02_delete_peer_21.update.bin" => deleting.export_updates(since: delete_common),
    "03_set_peer_22.update.bin" => setting.export_updates(since: delete_common)
  }
)

dependent = Loro::Doc.new(peer_id: 30)
dependent.get_map("document").set("first", 1)
first_update = dependent.export_updates
after_first = dependent.version_vector
dependent.get_map("document").set("second", 2)
second_update = dependent.export_updates(since: after_first)
write_case(
  "pending_dependency",
  manifest: {
    "peers" => [30],
    "description" => "The second update depends on the first and may be delivered before it.",
    "operations" => {
      "30" => [["document", "set", "first", 1], ["document", "set", "second", 2]]
    }
  },
  blobs: {
    "01_first_peer_30.update.bin" => first_update,
    "02_dependent_peer_30.update.bin" => second_update
  }
)

three_actor_blobs = (41..43).to_h do |peer|
  operations = [["document", :set, "peer_#{peer}", peer], ["document", :set, "winner", peer]]
  [format("%02d_peer_%d.update.bin", peer - 40, peer), independent_actor(peer, operations)]
end
write_case(
  "three_actor_conflict",
  manifest: {
    "peers" => [41, 42, 43],
    "description" => "Three independent actors preserve unique keys and deterministically resolve a shared key.",
    "operations" => {
      "41" => [["document", "set", "peer_41", 41], ["document", "set", "winner", 41]],
      "42" => [["document", "set", "peer_42", 42], ["document", "set", "winner", 42]],
      "43" => [["document", "set", "peer_43", 43], ["document", "set", "winner", 43]]
    }
  },
  blobs: three_actor_blobs
)
