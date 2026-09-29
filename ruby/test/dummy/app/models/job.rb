class Job < ApplicationRecord
  belongs_to :user

  enum :priority, { low: 'low', high: 'high' }, validate: true

  validates :state, inclusion: { in: %w[queued running done] }
  validates :state, inclusion: { in: %w[conditional] }, if: -> { false }
end
