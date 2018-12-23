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
require "yaml"
require "./archive/*"

class Config
  YAML.mapping({
    db: NamedTuple(
      user: String,
      password: String,
      host: String,
      port: Int32,
      dbname: String,
    ),
    access_key:        String,
    secret_key:        String,
    region:            String,
    bucket:            String,
    endpoint:          String,
    content_threshold: Float64,
  })
end

CONFIG = Config.from_yaml(File.read("config/config.yml"))

ACCESS_KEY      = CONFIG.access_key
SECRET_KEY      = CONFIG.secret_key
REGION          = CONFIG.region
BUCKET          = CONFIG.bucket
SPACES_ENDPOINT = CONFIG.endpoint

CONTENT_THRESHOLD = CONFIG.content_threshold

PG_URL = URI.new(
  scheme: "postgres",
  user: CONFIG.db[:user],
  password: CONFIG.db[:password],
  host: CONFIG.db[:host],
  port: CONFIG.db[:port],
  path: CONFIG.db[:dbname],
)

PG_DB = DB.open PG_URL

class Worker
  DB.mapping({
    id:            String,
    ip:            String,
    reputation:    Int32,
    disabled:      Bool,
    current_batch: String?,
  })
end

class Batch
  DB.mapping({
    id:           String,
    start_ctid:   String,
    end_ctid:     String,
    finished:     Bool,
    content_size: Int32?,
  })
end

post "/api/workers/create" do |env|
  env.response.content_type = "application/json"

  remote_address = env.as(HTTP::Server::NewContext).remote_address.address
  worker_count = PG_DB.query_one("SELECT count(*) FROM workers WHERE ip = $1", remote_address, as: Int64)

  if worker_count > 10
    response = {
      "error"      => "Too many workers for IP",
      "error_code" => 1,
    }.to_json
    halt env, status_code: 403, response: response
  end

  worker_id = "#{UUID.random}"
  PG_DB.exec("INSERT INTO workers VALUES ($1, $2, $3, $4)", worker_id, remote_address, 0, false)

  response = {
    "worker_id" => worker_id,
    "s3_url"    => "https://#{BUCKET}.#{REGION}.#{SPACES_ENDPOINT}",
  }.to_json
  halt env, status_code: 200, response: response
end

post "/api/batches" do |env|
  env.response.content_type = "application/json"

  worker_id = env.params.body["worker_id"]
  worker = PG_DB.query_one?("SELECT * FROM workers WHERE id = $1", worker_id, as: Worker)

  if !worker
    response = {
      "error"      => "Worker does not exist",
      "error_code" => 2,
    }.to_json
    halt env, status_code: 403, response: response
  end

  if worker.disabled
    response = {
      "error"      => "Worker is disabled",
      "error_code" => 3,
    }.to_json
    halt env, status_code: 403, response: response
  end

  if worker.current_batch
    response = {
      "error"      => "Worker must commit #{worker.current_batch}",
      "error_code" => 4,
      "batch_id"   => worker.current_batch,
    }.to_json
    halt env, status_code: 403, response: response
  end

  # Check trusted workers less often
  if rand(worker.reputation + 1) == 0 && PG_DB.query_one("SELECT count(*) FROM batches WHERE finished = true", as: Int64) != 0
    select_finished = true
  else
    select_finished = false
  end

  batch_id, start_ctid, end_ctid = PG_DB.query_one("SELECT id, start_ctid, end_ctid FROM batches WHERE finished = $1 ORDER BY RANDOM() LIMIT 1", select_finished, as: {String, String, String})
  objects = PG_DB.query_all("SELECT id FROM videos WHERE ctid >= $1 AND ctid <= $2", start_ctid, end_ctid, as: String)

  # Assign worker with batch
  PG_DB.exec("UPDATE workers SET current_batch = $1 WHERE id = $2", batch_id, worker_id)

  response = {
    "batch_id" => batch_id,
    "objects"  => objects,
  }.to_json
  halt env, status_code: 200, response: response
end

post "/api/batches/:batch_id" do |env|
  env.response.content_type = "application/json"

  worker_id = env.params.body["worker_id"]
  batch_id = env.params.url["batch_id"]

  worker = PG_DB.query_one?("SELECT * FROM workers WHERE id = $1", worker_id, as: Worker)

  if !worker
    response = {
      "error"      => "Worker does not exist",
      "error_code" => 2,
    }.to_json
    halt env, status_code: 403, response: response
  end

  if worker.disabled
    response = {
      "error"      => "Worker is disabled",
      "error_code" => 3,
    }.to_json
    halt env, status_code: 403, response: response
  end

  if batch_id != worker.current_batch
    response = {
      "error"      => "Worker isn't allowed access to #{batch_id}",
      "error_code" => 5,
    }.to_json
    halt env, status_code: 403, response: response
  end

  start_ctid, end_ctid = PG_DB.query_one("SELECT start_ctid, end_ctid FROM batches WHERE id = $1", batch_id, as: {String, String})
  objects = PG_DB.query_all("SELECT id FROM videos WHERE ctid >= $1 AND ctid <= $2", start_ctid, end_ctid, as: String)

  response = {
    "batch_id" => batch_id,
    "objects"  => objects,
  }.to_json
  halt env, status_code: 200, response: response
end

post "/api/commit" do |env|
  env.response.content_type = "application/json"

  worker_id = env.params.body["worker_id"]
  batch_id = env.params.body["batch_id"]
  content_size = env.params.body["content_size"].try &.to_i?
  content_size ||= 0

  worker = PG_DB.query_one?("SELECT * FROM workers WHERE id = $1", worker_id, as: Worker)

  if !worker
    response = {
      "error"      => "Worker does not exist",
      "error_code" => 2,
    }.to_json
    halt env, status_code: 403, response: response
  end

  if worker.disabled
    response = {
      "error"      => "Worker is disabled",
      "error_code" => 3,
    }.to_json
    halt env, status_code: 403, response: response
  end

  if batch_id.empty?
    response = {
      "error"      => "Cannot commit with empty batch_id",
      "error_code" => 6,
    }.to_json
    halt env, status_code: 403, response: response
  end

  if batch_id != worker.current_batch
    response = {
      "error"      => "Worker must commit #{worker.current_batch}",
      "error_code" => 4,
      "batch_id"   => worker.current_batch,
    }.to_json
    halt env, status_code: 403, response: response
  end

  batch = PG_DB.query_one?("SELECT * FROM batches WHERE id = $1", worker.current_batch, as: Batch)

  if !batch
    response = {
      "error"      => "Batch #{worker.current_batch} does not exist",
      "error_code" => 7,
    }.to_json
    halt env, status_code: 403, response: response
  end

  if batch.finished && batch.content_size
    if ((content_size - batch.content_size.not_nil!).to_f / batch.content_size.not_nil!.to_f).abs < CONTENT_THRESHOLD
      PG_DB.exec("UPDATE workers SET reputation = reputation + 1, current_batch = NULL WHERE id = $1", worker_id)

      response = {
        "upload_url" => "",
      }.to_json
      halt env, status_code: 200, response: response
    else
      PG_DB.exec("UPDATE workers SET reputation = reputation - 5 WHERE id = $1", worker.id)
      PG_DB.exec("UPDATE workers SET disabled = true WHERE reputation < 0 AND id = $1", worker.id)

      response = {
        "error"      => "Invalid size for #{batch_id}",
        "error_code" => 8,
        "batch_id"   => batch.id,
      }.to_json
      halt env, status_code: 403, response: response
    end
  end

  options = Awscr::S3::Presigned::Url::Options.new(
    aws_access_key: ACCESS_KEY,
    aws_secret_key: SECRET_KEY,
    region: REGION,
    object: "#{batch.id}.json.gz",
    bucket: "",
    host_name: "#{BUCKET}.#{REGION}.#{SPACES_ENDPOINT}",
    additional_options: {
      "Content-Type"         => "application/gzip",
      "Content-Length-Range" => "#{(content_size - content_size * (CONTENT_THRESHOLD / 2)).to_i},#{(content_size + content_size * (CONTENT_THRESHOLD / 2)).to_i}",
    }
  )
  url = Awscr::S3::Presigned::Url.new(options).for(:put)

  response = {
    "upload_url" => url,
  }.to_json
  halt env, status_code: 200, response: response
end

post "/api/finalize" do |env|
  env.response.content_type = "application/json"

  worker_id = env.params.body["worker_id"]
  batch_id = env.params.body["batch_id"]

  worker = PG_DB.query_one?("SELECT * FROM workers WHERE id = $1", worker_id, as: Worker)

  if !worker
    response = {
      "error"      => "Worker does not exist",
      "error_code" => 2,
    }.to_json
    halt env, status_code: 403, response: response
  end

  if worker.disabled
    response = {
      "error"      => "Worker is disabled",
      "error_code" => 3,
    }.to_json
    halt env, status_code: 403, response: response
  end

  if batch_id.empty?
    response = {
      "error"      => "Cannot commit with empty batch_id",
      "error_code" => 6,
    }.to_json
    halt env, status_code: 403, response: response
  end

  if batch_id != worker.current_batch
    response = {
      "error"      => "Worker must commit #{worker.current_batch}",
      "error_code" => 4,
      "batch_id"   => worker.current_batch,
    }.to_json
    halt env, status_code: 403, response: response
  end

  batch = PG_DB.query_one?("SELECT * FROM batches WHERE id = $1", worker.current_batch, as: Batch)

  if !batch
    response = {
      "error"      => "Batch #{worker.current_batch} does not exist",
      "error_code" => 7,
    }.to_json
    halt env, status_code: 403, response: response
  end

  s3_signer = Awscr::S3::SignerFactory.get(version: :v4, region: REGION, aws_access_key: ACCESS_KEY, aws_secret_key: SECRET_KEY)
  s3_client = Awscr::S3::Http.new(signer: s3_signer, region: REGION, custom_endpoint: "https://#{BUCKET}.#{REGION}.#{SPACES_ENDPOINT}")
  response = s3_client.head("/#{batch.id}.json.gz")
  content_size = response.headers["Content-Length"].to_i

  PG_DB.exec("UPDATE batches SET content_size = $1, finished = $2 WHERE id = $3", content_size, true, batch.id)
  PG_DB.exec("UPDATE workers SET reputation = reputation + 1, current_batch = NULL WHERE id = $1", worker_id)

  halt env, status_code: 200
end

if PG_DB.query_one("SELECT count(*) FROM batches WHERE finished = true", as: Int64) == 0
  puts "WARNING: No completed batches, will not be able to verify workers"
end

# Add redirect if SSL is enabled
if Kemal.config.ssl
  spawn do
    server = HTTP::Server.new do |context|
      redirect_url = "https://#{context.request.host}#{context.request.path}"
      if context.request.query
        redirect_url += "?#{context.request.query}"
      end
      context.response.headers.add("Location", redirect_url)
      context.response.status_code = 301
    end

    server.bind_tcp "0.0.0.0", 80
    server.listen
  end
end

gzip true
Kemal.run
