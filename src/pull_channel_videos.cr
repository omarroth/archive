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
  PG_DB.query("SELECT ucid FROM channels WHERE video_count IS NULL AND joined < '2017-06-01'") do |rs|
    rs.each do
      ucid = rs.read(String)

      if active_threads >= max_threads
        if active_channel.receive
          active_threads -= 1
        end
      end

      active_threads += 1
      spawn do
        ids = [] of String
        page = 1
        client = HTTP::Client.new(YT_URL)

        loop do
          url = produce_channel_videos_url(ucid, page)
          response = client.get(url)

          done = false
          response.body.scan(/vi\\\/(?<video_id>[a-zA-Z0-9_-]{11})/) do |match|
            if ids.includes? match["video_id"]
              done = true
              break
            end

            ids << match["video_id"]
          end

          if page == 100
            done = true
          end

          if done
            break
          end

          page += 1
        end

        playlist_ids = [] of String
        page = 1
        client = HTTP::Client.new(YT_URL)

        loop do
          url = produce_playlist_url(ucid, page)
          response = client.get(url)

          done = false
          response.body.scan(/vi\\\/(?<video_id>[a-zA-Z0-9_-]{11})/) do |match|
            if playlist_ids.includes? match["video_id"]
              done = true
              break
            end

            playlist_ids << match["video_id"]
          end

          if response.body.scan(/vi\\\/(?<video_id>[a-zA-Z0-9_-]{11})/).size < 100
            done = true
          end

          if done
            ids |= playlist_ids
            break
          end

          page += 1
        end

        ids.uniq!
        video_count = ids.size
        if !ids.empty?
          ids = ids.map { |video| "('#{video}', false)" }.join(",")
          PG_DB.exec("INSERT INTO videos VALUES #{ids} ON CONFLICT DO NOTHING")
        end

        PG_DB.exec("UPDATE channels SET video_count = $1 WHERE ucid = $2", video_count, ucid)
        active_channel.send(true)
      end

      remaining = PG_DB.query_one("SELECT count(*) FROM channels WHERE video_count IS NULL AND joined < '2017-06-01'", as: Int64)
      finished = PG_DB.query_one("SELECT count(*) FROM channels WHERE video_count IS NOT NULL AND joined < '2017-06-01'", as: Int64)
      print "Remaining: #{remaining}, processed: #{finished}\r"
    end
  end
end

def produce_channel_videos_url(ucid, page = 1, auto_generated = nil, sort_by = "newest")
  if auto_generated
    seed = Time.unix(1525757349)

    until seed >= Time.now
      seed += 1.month
    end
    timestamp = seed - (page - 1).months

    page = "#{timestamp.to_unix}"
    switch = "\x36"
  else
    page = "#{page}"
    switch = "\x00"
  end

  meta = "\x12\x06videos"
  meta += "\x30\x02"
  meta += "\x38\x01"
  meta += "\x60\x01"
  meta += "\x6a\x00"
  meta += "\xb8\x01\x00"
  meta += "\x20#{switch}"
  meta += "\x7a"
  meta += page.size.to_u8.unsafe_chr
  meta += page

  case sort_by
  when "newest"
    # Empty tags can be omitted
    # meta += "\x18\x00"
  when "popular"
    meta += "\x18\x01"
  when "oldest"
    meta += "\x18\x02"
  end

  meta = Base64.urlsafe_encode(meta)
  meta = URI.escape(meta)

  continuation = "\x12"
  continuation += ucid.size.to_u8.unsafe_chr
  continuation += ucid
  continuation += "\x1a"
  continuation += meta.size.to_u8.unsafe_chr
  continuation += meta

  continuation = continuation.size.to_u8.unsafe_chr + continuation
  continuation = "\xe2\xa9\x85\xb2\x02" + continuation

  continuation = Base64.urlsafe_encode(continuation)
  continuation = URI.escape(continuation)

  url = "/browse_ajax?continuation=#{continuation}&gl=US&hl=en"

  return url
end

def produce_playlist_url(id, index)
  if id.starts_with? "UC"
    id = "UU" + id.lchop("UC")
  end
  ucid = "VL" + id

  meta = [0x08_u8] + write_var_int(index)
  meta = Slice.new(meta.to_unsafe, meta.size)
  meta = Base64.urlsafe_encode(meta, false)
  meta = "PT:#{meta}"

  wrapped = "\x7a"
  wrapped += meta.bytes.size.unsafe_chr
  wrapped += meta

  wrapped = Base64.urlsafe_encode(wrapped)
  meta = URI.escape(wrapped)

  continuation = "\x12"
  continuation += ucid.size.unsafe_chr
  continuation += ucid
  continuation += "\x1a"
  continuation += meta.bytes.size.unsafe_chr
  continuation += meta

  continuation = continuation.size.to_u8.unsafe_chr + continuation
  continuation = "\xe2\xa9\x85\xb2\x02" + continuation

  continuation = Base64.urlsafe_encode(continuation)
  continuation = URI.escape(continuation)

  url = "/browse_ajax?continuation=#{continuation}"

  return url
end

def write_var_int(value : Int)
  bytes = [] of UInt8
  value = value.to_u32

  if value == 0
    bytes = [0_u8]
  else
    while value != 0
      temp = (value & 0b01111111).to_u8
      value = value >> 7

      if value != 0
        temp |= 0b10000000
      end

      bytes << temp
    end
  end

  return bytes
end

def fetch_playlists(ucid)
  client = HTTP::Client.new(YT_URL)
  response = client.get("/channel/#{ucid}/playlists?disable_polymer=1")
  playlists = [] of String

  response.body.scan(/\/playlist\?list=(?<playlist_id>[^"]+)/) do |match|
    playlists << match["playlist_id"]
  end

  loop do
    if match = response.body.match(/\/browse_ajax\?[^"]+/)
      continuation = match[0]
      response = client.get("/channel/#{ucid}/playlists?disable_polymer=1")

      response.body.scan(/\/playlist\?list=(?<playlist_id>[^"]+)/) do |match|
        playlists << match["playlist_id"]
      end
    else
      break
    end
  end

  playlists << ucid

  return playlists
end
