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

loop do
  PG_DB.query("SELECT id FROM videos WHERE finished = false OR published IS NULL") do |rs|
    rs.each do
      id = rs.read(String)

      if active_threads >= max_threads
        if active_channel.receive
          active_threads -= 1
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

          published = html.xpath_node(%q(//meta[@itemprop="datePublished"])).try &.["content"]
          if published
            published = Time.parse(published, "%Y-%m-%d", Time::Location.local)
          end
          published ||= Time.now

          recommended_videos = html.xpath_nodes(%q(//*[@data-vid])).map do |node|
            node["data-vid"]
          end

          if !recommended_videos.empty?
            recommended_videos = recommended_videos.map { |channel| "('#{channel}', false)" }.join(",")
            PG_DB.exec("INSERT INTO videos VALUES #{recommended_videos} ON CONFLICT DO NOTHING")
          end

          uploader = html.xpath_node(%q(//*[@data-channel-external-id])).try &.["data-channel-external-id"]
          if uploader
            PG_DB.exec("INSERT INTO channels VALUES ($1, $2) ON CONFLICT (ucid) DO NOTHING", uploader, true)
          end

          session_token = response.body.match(/'XSRF_TOKEN': "(?<session_token>[A-Za-z0-9\_\-\=]+)"/)
          itct = response.body.match(/itct=(?<itct>[^"]+)"/)
          ctoken = response.body.match(/'COMMENTS_TOKEN': "(?<ctoken>[^"]+)"/)

          if session_token && itct && ctoken && response
            session_token = session_token["session_token"]
            itct = itct["itct"]
            ctoken = ctoken["ctoken"]

            request = HTTP::Params.encode({
              "session_token" => session_token,
            })

            headers = HTTP::Headers.new
            headers["cookie"] = response.cookies.add_request_headers(headers)["cookie"]
            headers["content-type"] = "application/x-www-form-urlencoded"

            headers["x-client-data"] = "CIi2yQEIpbbJAQipncoBCNedygEIqKPKAQ=="
            headers["x-spf-previous"] = "https://www.youtube.com/watch?v=#{id}&gl=US&hl=en&disable_polymer=1&has_verified=1&bpctr=9999999999"
            headers["x-spf-referer"] = "https://www.youtube.com/watch?v=#{id}&gl=US&hl=en&disable_polymer=1&has_verified=1&bpctr=9999999999"

            headers["x-youtube-client-name"] = "1"
            headers["x-youtube-client-version"] = "2.20180719"

            response = client.post("/comment_service_ajax?action_get_comments=1&pbj=1&ctoken=#{ctoken}&continuation=#{ctoken}&itct=#{itct}&hl=en&gl=US", headers, request)

            ucids = [] of String
            response.body.scan(/"(?<channel_id>UC[a-zA-Z0-9_-]{22})"/) do |match|
              ucids << match["channel_id"]
            end

            if !ucids.empty?
              ucids = ucids.map { |channel| "('#{channel}', false)" }.join(",")
              PG_DB.exec("INSERT INTO channels VALUES #{ucids} ON CONFLICT DO NOTHING")
            end
          end

          PG_DB.exec("UPDATE videos SET finished = true, published = $2 WHERE id = $1", id, published)
        rescue ex
        end

        active_channel.send(true)
      end

      remaining = PG_DB.query_one("SELECT count(*) FROM videos WHERE finished = false", as: Int64)
      finished = PG_DB.query_one("SELECT count(*) FROM videos WHERE finished = true", as: Int64)
      print "Remaining: #{remaining}, processed: #{finished}\r"
    end
  end
end
