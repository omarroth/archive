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
PG_DB.exec("DECLARE C CURSOR FOR SELECT id, ctid FROM videos WHERE finished = false")

i = 0
loop do
  batch = PG_DB.query_all("FETCH #{BATCH_SIZE} FROM C", as: {String, Slice(UInt8)})
  start_ctid, end_ctid = {batch[0][1], batch[-1][1]}.map do |slice|
    major = IO::ByteFormat::BigEndian.decode(UInt32, slice)
    slice += 4
    minor = IO::ByteFormat::BigEndian.decode(UInt16, slice)

    "(#{major},#{minor})"
  end

  batch = batch.map { |id, ctid| id }

  if batch.size < BATCH_SIZE
    break
  end

  PG_DB.exec("UPDATE videos SET finished = true WHERE id = ANY('{#{batch.join(",")}}')")
  PG_DB.exec("INSERT INTO batches VALUES ($1, $2, $3, $4, $5, $6)", UUID.random, start_ctid, end_ctid, false, nil, batch)
  i += 1

  print "Created #{i} new batches\r"
end

PG_DB.exec("COMMIT WORK")

puts "Created #{i} new batches."
