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

PG_DB.exec("INSERT INTO channels VALUES ('UCu6mSoMNzHQiBIOCkHUa2Aw', false) ON CONFLICT DO NOTHING")
max_threads = 40

OptionParser.parse! do |parser|
  parser.on("-t THREADS", "--threads=THREADS", "Number of threads to use for crawling") { |threads| max_threads = threads.to_i }
end

active_threads = 0
active_channel = Channel(Bool).new

loop do
  PG_DB.query("SELECT ucid FROM channels WHERE finished = false OR joined IS NULL") do |rs|
    rs.each do
      ucid = rs.read(String)

      if active_threads >= max_threads
        if active_channel.receive
          active_threads -= 1
        end
      end

      active_threads += 1
      spawn do
        client = HTTP::Client.new(YT_URL)

        begin
          response = client.get("/channel/#{ucid}/about?disable_polymer=1")
          body = response.body
          status_code = response.status_code
        rescue ex
          body ||= "<html></html>"
          status_code ||= 500
        end

        if status_code == 200
          response = XML.parse_html(body)

          joined = response.xpath_node(%q(//span[contains(text(), "Joined")]))
          if joined
            joined = Time.parse(joined.content.lchop("Joined "), "%b %-d, %Y", Time::Location.local)
          end
          joined ||= "2005-01-01"

          related_channels = response.xpath_nodes(%q(//div[contains(@class, "branded-page-related-channels")]/ul/li))
          related_channels = related_channels.map do |node|
            node["data-external-id"]
          end
          related_channels ||= [] of String

          if !related_channels.empty?
            related_channels = related_channels.map { |channel| "('#{channel}', false)" }.join(",")
            PG_DB.exec("INSERT INTO channels VALUES #{related_channels} ON CONFLICT DO NOTHING")
          end

          PG_DB.exec("UPDATE channels SET finished = true, joined = $2 WHERE ucid = $1", ucid, joined)
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
