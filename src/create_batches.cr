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

PG_DB.exec("BEGIN WORK")
PG_DB.exec("DECLARE C CURSOR FOR SELECT id FROM videos WHERE finished = false")

i = 0
loop do
  batch = PG_DB.query_all("FETCH #{BATCH_SIZE} FROM C", as: String)

  if batch.size < BATCH_SIZE
    break
  end

  PG_DB.exec("INSERT INTO batches VALUES ($1, $2, $3, $4, $5, $6)", UUID.random, "(0,0)", "(0,0)", false, nil, batch)
  PG_DB.exec("UPDATE videos SET finished = true WHERE id = ANY('{#{batch.join(",")}}')")
  i += 1

  print "Created #{i} new batches\r"
end

PG_DB.exec("COMMIT WORK")

puts "Created #{i} new batches."
