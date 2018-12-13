# "Annotation Archive" (which provides scripts for archiving YouTube annotations)
# Copyright (C) 2018  Omar Roth
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

require "awscr-s3"
require "json"
require "kemal"
require "pg"
require "./archive/*"

CONFIG = Config.from_yaml(File.read("config/config.yml"))

PG_URL = URI.new(
  scheme: "postgres",
  user: CONFIG.db[:user],
  password: CONFIG.db[:password],
  host: CONFIG.db[:host],
  port: CONFIG.db[:port],
  path: CONFIG.db[:dbname],
)

PG_DB      = DB.open PG_URL
BATCH_SIZE = 10000

index = 0
get "/batch" do |env|
  size = env.params.query["size"]?.try &.to_i?
  size ||= BATCH_SIZE

  env.response.content_type = "application/json"

  response = PG_DB.query_all("SELECT id FROM videos LIMIT $1 OFFSET $2", size, index, as: String).to_json
  index += size

  response
end

gzip true
Kemal.run
