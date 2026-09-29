class User < ApplicationRecord
  has_many :boards
  has_many :jobs
end
