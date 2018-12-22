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

PG_DB = DB.open PG_URL

get "/api/batches" do |env|
  env.response.content_type = "application/json"

  batch_id, start_ctid, end_ctid = PG_DB.query_one("SELECT id, start_ctid, end_ctid FROM batches WHERE finished = false ORDER BY RANDOM() LIMIT 1", as: {String, String, String})
  objects = PG_DB.query_all("SELECT id FROM videos WHERE ctid >= $1 AND ctid <= $2", start_ctid, end_ctid, as: String)

  response = JSON.build do |json|
    json.object do
      json.field "batch_id", batch_id
      json.field "objects", objects
    end
  end

  JSON.parse(response).to_pretty_json
  # response
end

post "/api/workers/create" do |env|
  env.response.content_type = "application/json"

  remote_address = env.as(HTTP::Server::NewContext).remote_address.address

  worker_count = PG_DB.query_one("SELECT count(*) FROM workers WHERE ip = $1", remote_address, as: Int64)
  if worker_count > 10
    response = JSON.build do |json|
      json.object do
        json.field "error", "Too many workers for IP"
      end
    end

    next response
  end

  worker_id = "#{UUID.random}"
  PG_DB.exec("INSERT INTO workers VALUES ($1, $2, $3, $4)", worker_id, remote_address, 0, false)

  response = JSON.build do |json|
    json.object do
      json.field "worker_id", worker_id
    end
  end

  response
end

post "/api/commit" do |env|
  # ...
end

gzip true
Kemal.run
