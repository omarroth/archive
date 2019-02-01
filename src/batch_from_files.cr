require "pg"
require "uuid"
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

PG_DB      = DB.open PG_URL
BATCH_SIZE = 10000

buffer = [] of String
i = 0
File.each_line("shared.csv") do |line|
  buffer << line

  if buffer.size == BATCH_SIZE
    batch_id = UUID.random

    PG_DB.exec("INSERT INTO batches VALUES ($1, $2, $3, $4, $5, $6, $7)", UUID.random, "(0,0)", "(0,0)", false, nil, buffer, 0)
    buffer.clear

    i += 1
    print "Created #{i} new batches\r"
  end
end

puts "Created #{i} new batches."

path_unfinished = "videos-unfinished.csv"

buffer.each do |id|
  File.write(path_unfinished, "#{id}\n", mode: "a")
end
puts "Wrote unfinished videos to #{path_unfinished}"
