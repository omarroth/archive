require "awscr-s3"
require "gzip"
require "http/client"
require "json"
require "./archive/*"

CONFIG          = WorkerConfig.from_yaml(File.read("config/worker_config.yml"))
ACCESS_KEY      = CONFIG.access_key
SECRET_KEY      = CONFIG.secret_key
BATCH_URL       = URI.parse(CONFIG.batch_url)
BUCKET          = "youtube-annotation-archive"
REGION          = "sfo2"
SPACES_ENDPOINT = "https://sfo2.digitaloceanspaces.com"
YT_URL          = URI.parse("https://www.youtube.com")

batch_size = 10000
batch_size ||= CONFIG.batch_size

s3 = Awscr::S3::Client.new(REGION, ACCESS_KEY, SECRET_KEY, endpoint: SPACES_ENDPOINT)

loop do
  begin
    batch_client = HTTP::Client.new(BATCH_URL)
    batch_response = batch_client.get("/batch")
  rescue ex
    sleep 10.seconds
    next
  end

  if batch_response.status_code == 200
    ids = JSON.parse(batch_response.body).as_a
  else
    # Slow down if something went wrong
    sleep 10.seconds
    next
  end

  if ids.empty?
    break
  end

  filename = "#{ids[0]}-#{ids[-1]}.json.gz"
  options = {
    "x-amz-acl"    => "public-read",
    "content-type" => "application/gzip",
  }

  client = HTTP::Client.new(YT_URL)
  annotations = {} of String => String
  errors = [] of String

  ids.each do |id|
    id = id.as_s

    begin
      response = client.get("/annotations_invideo?video_id=#{id}")
      if response.status_code == 200
        annotations[id] = response.body
      else
        errors << id
      end
    rescue ex
      errors << id
    end
  end

  Gzip::Writer.open(io = IO::Memory.new) do |gzip|
    gzip.write(annotations.to_json.to_slice)
  end

  io.rewind
  content = io.gets_to_end

  resp = s3.put_object(BUCKET, filename, content, options)
  puts resp.etag

  if !errors.empty?
    resp = s3.put_object(BUCKET, "error-#{errors[0]}.json", errors.to_json, {"x-amz-acl"    => "public-read",
                                                                             "content-type" => "application/json"})
  end
end
