require "gzip"
require "http/client"
require "json"
require "option_parser"

YT_URL = URI.parse("https://www.youtube.com")

batch_url = URI.parse("http://localhost:3000")
max_threads = 10

OptionParser.parse! do |parser|
  parser.on("-u URL", "--batch-url=URL", "Master server URL") { |url| batch_url = URI.parse(url) }
  parser.on("-t THREADS", "--max-threads=THREADS", "Number of threads for downloading annotations") { |threads| max_threads = threads.to_i }
end

def make_client(url)
  client = HTTP::Client.new(url)
  client.read_timeout = 10.seconds
  client.connect_timeout = 10.seconds

  return client
end

batch_headers = HTTP::Headers.new
batch_headers["Content-Type"] = "application/json"
batch_client = make_client(batch_url)

if File.exists? ".worker_info"
  body = JSON.parse(File.read(".worker_info"))

  worker_id = body["worker_id"].as_s
  s3_url = body["s3_url"].as_s
else
  response = batch_client.post("/api/workers/create", batch_headers)
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
s3_client = make_client(s3_url)

response = HTTP::Client::Response.new(500)
body = JSON::Any.new(nil)

loop do
  begin
    response = batch_client.post("/api/batches", batch_headers, body: {
      "worker_id" => worker_id,
    }.to_json)
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

      response = batch_client.post("/api/batches/#{batch_id}", batch_headers, body: {
        "worker_id" => worker_id,
      }.to_json)
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

  annotations = {} of String => String

  active_threads = 0
  active_channel = Channel({String, String}).new

  # Main loop
  objects.each do |id|
    video_id = id.as_s

    if active_threads >= max_threads
      if thread_response = active_channel.receive
        response_id, response_body = thread_response

        annotations[response_id] = response_body
        active_threads -= 1
        print "Got annotations for #{annotations.keys.size}/#{objects.size} videos    \r"
      end
    end

    active_threads += 1
    spawn do
      loop do
        begin
          yt_client = make_client(YT_URL)
          response = yt_client.get("/annotations_invideo?video_id=#{id}&gl=US&hl=en")

          if response.status_code == 200
            active_channel.send({video_id, response.body})
          else
            active_channel.send({video_id, ""})
          end

          break
        rescue
        end
      end
    end
  end

  content = annotations.to_json.to_slice
  uncompressed_size = content.size
  puts "All annotations collected (#{(uncompressed_size.to_f / 1024**2).round(1)} MiB)    "

  puts "Compressing..."

  Gzip::Writer.open(io = IO::Memory.new) do |gzip|
    gzip.write(content)
  end

  io.rewind
  content = io.gets_to_end
  content_size = content.size

  puts "Committing..."

  loop do
    begin
      batch_client = make_client(batch_url)
      response = batch_client.post("/api/commit", batch_headers, body: {
        "worker_id"    => worker_id,
        "batch_id"     => batch_id,
        "content_size" => content_size,
      }.to_json)

      break
    rescue ex
    end
  end

  body = JSON.parse(response.body)

  if response.status_code == 200
    upload_url = body["upload_url"].as_s
  else
    error = body["error"].as_s
    puts error
    break
  end

  if upload_url.empty?
    puts "No need to upload #{batch_id}, all done!"
    next
  end

  puts "All annotations compressed (#{(content_size.to_f / 1024**2).round(1)} MiB)    "
  puts "Uploading to S3..."
  loop do
    begin
      response = s3_client.put(upload_url, body: content)
      break
    rescue ex
    end
  end

  if response.status_code == 200
    response = batch_client.post("/api/finalize", batch_headers, body: {
      "worker_id" => worker_id,
      "batch_id"  => batch_id,
    }.to_json)

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
