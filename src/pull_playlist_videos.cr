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
active_channel = Channel(String).new
i = 0

File.each_line("playlists.csv") do |plid|
  if active_threads >= max_threads
    if playlist = active_channel.receive
      active_threads -= 1
      i += 1
      print "Processed: #{i}\r"
      File.write("finished_playlists.csv", "#{playlist}\n", mode: "a")
    end
  end

  active_threads += 1
  spawn do
    videos = [] of String
    index = 0
    client = HTTP::Client.new(YT_URL)

    loop do
      url = produce_playlist_url(plid, index)
      response = client.get(url)

      done = false
      response.body.scan(/vi\\\/(?<video_id>[a-zA-Z0-9_-]{11})/) do |match|
        if videos.includes? match["video_id"]
          done = true
          break
        end

        videos << match["video_id"]
      end

      if response.body.scan(/vi\\\/(?<video_id>[a-zA-Z0-9_-]{11})/).size < 100
        done = true
      end

      if done
        break
      end

      index += 100
    end

    videos.uniq!

    if !videos.empty?
      videos = videos.map { |video| "('#{video}', false)" }.join(",")
      PG_DB.exec("INSERT INTO videos VALUES #{videos} ON CONFLICT (id) DO NOTHING")
    end

    active_channel.send(plid)
  end
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

  url = "/browse_ajax?continuation=#{continuation}&gl=US&hl=en"

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
