require "http/client"
require "json"
require "option_parser"
require "pg"
require "uri"
require "xml"
require "./archive/helpers/*"

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

PG_DB.exec("INSERT INTO channels VALUES ('UCu6mSoMNzHQiBIOCkHUa2Aw', false) ON CONFLICT DO NOTHING")
max_threads = 40

OptionParser.parse! do |parser|
  parser.on("-t THREADS", "--threads=THREADS", "Number of threads to use for crawling") { |threads| max_threads = threads.to_i }
end

active_threads = 0
active_channel = Channel(Bool).new

loop do
  PG_DB.query("SELECT ucid FROM channels WHERE finished = false") do |rs|
    rs.each do
      current = rs.read(String)

      if active_threads >= max_threads
        if active_channel.receive
          active_threads -= 1
        end
      end

      active_threads += 1
      spawn do
        begin
          related_channels = pull_related_channels(current)
        rescue ex
          related_channels = [] of String
        end

        PG_DB.exec("INSERT INTO channels VALUES ($1, $2) ON CONFLICT (ucid) DO UPDATE SET finished = true", current, true)
        if !related_channels.empty?
          related_channels = related_channels.map { |channel| "('#{channel}', false)" }.join(",")
          PG_DB.exec("INSERT INTO channels VALUES #{related_channels} ON CONFLICT DO NOTHING")
        end

        active_channel.send(true)
      end

      remaining = PG_DB.query_one("SELECT count(*) FROM channels WHERE finished = false", as: Int64)
      finished = PG_DB.query_one("SELECT count(*) FROM channels WHERE finished = true", as: Int64)
      print "Remaining: #{remaining}, processed: #{finished}\r"
    end
  end
end

def pull_related_channels(ucid)
  client = HTTP::Client.new(YT_URL)
  response = client.get("/channel/#{ucid}?disable_polymer=1")

  if response.status_code == 200
    response = XML.parse_html(response.body)
    related_channels = response.xpath_nodes(%q(//div[contains(@class, "branded-page-related-channels")]/ul/li))
    related_channels = related_channels.map do |node|
      node["data-external-id"]
    end
  end

  related_channels ||= [] of String

  return related_channels
end
