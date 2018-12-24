require "gzip"
require "http/client"
require "json"
require "option_parser"

YT_URL = URI.parse("https://www.youtube.com")

batch_url = URI.parse("http://localhost:3000")
OptionParser.parse! do |parser|
  parser.on("-u URL", "--batch-url=URL", "Master server URL") { |url| batch_url = URI.parse(url) }
end

batch_client = HTTP::Client.new(batch_url)

if File.exists? ".worker_info"
  body = JSON.parse(File.read(".worker_info"))

  worker_id = body["worker_id"].as_s
  s3_url = body["s3_url"].as_s
else
  response = batch_client.post("/api/workers/create")
  body = JSON.parse(response.body)

  if response.status_code == 200
    worker_id = body["worker_id"].as_s
    s3_url = body["s3_url"].as_s

    File.write(".worker_info", response.body)
  else
    raise body["error"].as_s
  end
end

s3_url = URI.parse(s3_url)
s3_client = HTTP::Client.new(s3_url)

response = HTTP::Client::Response.new(500)
body = JSON::Any.new(nil)

loop do
  begin
    response = batch_client.post("/api/batches", form: {
      "worker_id" => worker_id,
    })
  rescue ex
    next
  end

  body = JSON.parse(response.body)

  if response.status_code == 200
    batch_id = body["batch_id"].as_s
    objects = body["objects"].as_a
  else
    error = body["error"].as_s
    error_code = body["error_code"].as_i

    if error_code == 4
      batch_id = body["batch_id"].as_s
      puts "Continuing #{batch_id}..."

      response = batch_client.post("/api/batches/#{batch_id}", form: {
        "worker_id" => worker_id,
      })
      body = JSON.parse(response.body)

      if response.status_code == 200
        batch_id = body["batch_id"].as_s
        objects = body["objects"].as_a
      else
        error = body["error"].as_s
        puts error
        break
      end
    else
      puts error
      break
    end
  end

  yt_client = HTTP::Client.new(YT_URL)
  annotations = {} of String => String

  objects.each do |id|
    id = id.as_s

    begin
      response = yt_client.get("/annotations_invideo?video_id=#{id}&gl=US&hl=en")
      if response.status_code == 200
        annotations[id] = response.body
      else
        annotations[id] = ""
      end
    rescue ex
    end
  end

  Gzip::Writer.open(io = IO::Memory.new) do |gzip|
    gzip.write(annotations.to_json.to_slice)
  end

  io.rewind
  content = io.gets_to_end
  content_size = content.size

  response = batch_client.post("/api/commit", form: {
    "worker_id"    => worker_id,
    "batch_id"     => batch_id,
    "content_size" => "#{content_size}",
  })

  body = JSON.parse(response.body)

  if response.status_code == 200
    upload_url = body["upload_url"].as_s
  else
    error = body["error"].as_s
    puts error
    break
  end

  if upload_url.empty?
    next
  end

  response = s3_client.put(upload_url, body: content)

  if response.status_code == 200
    response = batch_client.post("/api/finalize", form: {
      "worker_id" => worker_id,
      "batch_id"  => batch_id,
    })

    if response.status_code == 200
      puts "Finished #{batch_id}"
    else
      body = JSON.parse(response.body)
      error = body["error"].as_s
      puts error
      break
    end
  else
    puts "Uploading to S3 failed, response: #{response.body}"
  end
end
