require "awscr-s3"
require "digest/md5"
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

s3 = Awscr::S3::Client.new(REGION, ACCESS_KEY, SECRET_KEY, endpoint: SPACES_ENDPOINT)
client = HTTP::Client.new(BATCH_URL)

if File.exists? ".worker_id"
  worker_id = File.read(".worker_id")
else
  resp = client.post("/api/workers/create")
  body = JSON.parse(resp.body)

  if resp.status_code == 200
    worker_id = body["worker_id"].as_s
    File.write(".worker_id", worker_id)
  else
    raise body["error"].as_s
  end
end

loop do
  begin
    response = client.get("/api/batches")
  rescue ex
    sleep 10.seconds
    next
  end

  if response.status_code == 200
    body = JSON.parse(response.body)

    batch_id = body["batch_id"].as_s
    objects = body["objects"].as_a
  else
    # Slow down if something went wrong
    sleep 10.seconds
    next
  end

  filename = "#{batch_id}.json.gz"
  options = {
    "x-amz-acl"    => "public-read",
    "content-type" => "application/gzip",
  }

  yt_client = HTTP::Client.new(YT_URL)
  annotations = {} of String => String

  objects.each do |id|
    id = id.as_s

    begin
      response = yt_client.get("/annotations_invideo?video_id=#{id}&gl=US&hl=en")
      if response.status_code == 200
        annotations[id] = response.body
      end
    rescue ex
    end
  end

  Gzip::Writer.open(io = IO::Memory.new) do |gzip|
    gzip.write(annotations.to_json.to_slice)
  end

  io.rewind
  content = io.gets_to_end

  md5_sum = Digest::MD5.hexdigest(content)
  puts md5_sum

  # body = {
  #   "..."
  # }
  # client.post("/api/commit", body)

  # resp = s3.put_object(BUCKET, filename, content, options)
  # puts resp.etag
end
