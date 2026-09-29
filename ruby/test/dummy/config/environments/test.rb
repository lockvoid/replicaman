Rails.application.configure do
  config.enable_reloading = false
  config.eager_load = ENV['CI'].present?
  config.consider_all_requests_local = true
  config.cache_store = :null_store
  config.active_support.deprecation = :stderr

  config.active_record.maintain_test_schema = false
end
