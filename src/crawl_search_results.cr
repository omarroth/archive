require "http/client"
require "json"
require "option_parser"
require "pg"
require "uri"
require "xml"
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

PG_DB  = DB.open PG_URL
YT_URL = URI.parse("https://www.youtube.com")

max_threads = 40

OptionParser.parse! do |parser|
  parser.on("-t THREADS", "--threads=THREADS", "Number of threads to use for crawling") { |threads| max_threads = threads.to_i }
end

active_threads = 0
active_channel = Channel(Int32).new
i = 0

loop do
  id = Random::Secure.urlsafe_base64(4)

  if active_threads >= max_threads
    if video_count = active_channel.receive
      active_threads -= 1
      i += video_count
    end
  end

  active_threads += 1
  spawn do
    client = HTTP::Client.new(YT_URL)
    videos = [] of String

    begin
      response = client.get("/results?search_query=#{id}&sp=EgIQAQ%3D%3D&disable_polymer=1")

      response.body.scan(/\/watch\?v=(?<video_id>[a-zA-Z0-9_-]{11})/) do |match|
        videos << match["video_id"]
      end

      videos = videos.map { |video| "('#{video}', false)" }.join(",")
      PG_DB.exec("INSERT INTO videos VALUES #{videos} ON CONFLICT DO NOTHING")
    rescue ex
    end

    active_channel.send(videos.size)
  end

  print "Processed: #{i}\r"
end
