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

PG_DB.exec("INSERT INTO videos VALUES ('CvFH_6DNRCY', false) ON CONFLICT DO NOTHING")
max_threads = 40

OptionParser.parse! do |parser|
  parser.on("-t THREADS", "--threads=THREADS", "Number of threads to use for crawling") { |threads| max_threads = threads.to_i }
end

active_threads = 0
active_channel = Channel(Bool).new
i = 0

PG_DB.exec("BEGIN")
PG_DB.exec("DECLARE crawl_videos CURSOR FOR SELECT id FROM videos")

loop do
  PG_DB.query("FETCH 100000 crawl_videos") do |rs|
    rs.each do
      id = rs.read(String)

      if active_threads >= max_threads
        if active_channel.receive
          active_threads -= 1
          i += 1
        end
      end

      active_threads += 1
      spawn do
        client = HTTP::Client.new(YT_URL)
        begin
          response = client.get("/watch?v=#{id}&gl=US&hl=en&disable_polymer=1&has_verified=1&bpctr=9999999999")

          if response.status_code == 200
            html = XML.parse_html(response.body)
          else
            raise "Invalid url"
          end

          videos = html.xpath_nodes(%q(//*[@data-vid])).map do |node|
            node["data-vid"]
          end

          if !videos.empty?
            videos = videos.map { |video| "('#{video}', false)" }.join(",")
            PG_DB.exec("INSERT INTO videos VALUES #{videos} ON CONFLICT (id) DO NOTHING")
          end

          uploader = html.xpath_node(%q(//*[@data-channel-external-id])).try &.["data-channel-external-id"]
          if uploader
            PG_DB.exec("INSERT INTO channels VALUES ($1, $2) ON CONFLICT (ucid) DO NOTHING", uploader, false)
          end
        rescue ex
        end

        active_channel.send(true)
      end

      print "Processed: #{i}\r"
    end
  end
end

PG_DB.exec("COMMIT")
