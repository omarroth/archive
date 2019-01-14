require "awscr-s3"
require "gzip"
require "json"
require "option_parser"
require "pg"
require "yaml"
require "./archive/*"

module Awscr::S3::Response
  class ListObjectsV2
    # Create a `ListObjectsV2` response from an
    # `HTTP::Client::Response` object
    def self.from_response(response)
      xml = XML.new(response.body)

      name = xml.string("//ListBucketResult/Name")
      prefix = xml.string("//ListBucketResult/Prefix")
      key_count = xml.string("//ListBucketResult/KeyCount")
      max_keys = xml.string("//ListBucketResult/MaxKeys")
      truncated = xml.string("//ListBucketResult/IsTruncated")
      token = xml.string("//ListBucketResult/NextContinuationToken")

      objects = [] of Object
      xml.array("ListBucketResult/Contents") do |object|
        key = object.string("Key")
        size = object.string("Size").to_i
        etag = object.string("ETag")

        objects << Object.new(key, size, etag)
      end

      new(name, prefix, key_count.to_i? || 0, max_keys.to_i, truncated == "true", token, objects)
    end

    # The list of obects
    getter contents

    def initialize(@name : String, @prefix : String, @key_count : Int32,
                   @max_keys : Int32, @truncated : Bool, @continuation_token : String, @contents : Array(Object))
    end

    # The continuation token for the subsequent response, if any
    def next_token
      @continuation_token
    end

    # Returns true if the response is truncated, false otherwise
    def truncated?
      @truncated
    end

    def_equals @name, @prefix, @key_count, @max_keys, @truncated,
      @continuation_token, @contents
  end
end

class Config
  YAML.mapping({
    db: NamedTuple(
      user: String,
      password: String,
      host: String,
      port: Int32,
      dbname: String,
    ),
    access_key:        String,
    secret_key:        String,
    region:            String,
    bucket:            String,
    endpoint:          String,
    content_threshold: Float64,
  })
end

CONFIG = Config.from_yaml(File.read("config/config.yml"))

ACCESS_KEY      = CONFIG.access_key
SECRET_KEY      = CONFIG.secret_key
REGION          = CONFIG.region
BUCKET          = CONFIG.bucket
SPACES_ENDPOINT = CONFIG.endpoint

s3_client = Awscr::S3::Client.new(REGION, ACCESS_KEY, SECRET_KEY, endpoint: "https://#{REGION}.#{SPACES_ENDPOINT}")
finished_batches = File.read("completed.txt").split("\n")

s3_client.list_objects(BUCKET, max_keys: 120000).each do |resp|
  resp.contents.map(&.key).each do |key|
    if finished_batches.includes? key
      puts "Skipping #{key}"
      next
    end

    object = s3_client.get_object(BUCKET, key)

    case object.body
    when IO
      io = object.body
    when String
      io = IO::Memory.new(object.body.as(String))
    else
      raise "Not valid body"
    end

    videos = [] of String

    Gzip::Reader.open(io) do |gzip|
      gzip.gets_to_end.scan(/[?&]v=(?<video_id>[a-zA-Z0-9_-]{11})/) do |match|
        if !videos.includes? match["video_id"]
          videos << match["video_id"]
        end
      end
    end

    puts "Adding #{videos.size} videos..."
    File.write("annotation-videos", videos.join("\n") + "\n", mode: "a")
    File.write("completed.txt", "#{key}\n", mode: "a")
    finished_batches << key
  end
end
