require "yaml"

class Config
  YAML.mapping({
    db: NamedTuple(
      user: String,
      password: String,
      host: String,
      port: Int32,
      dbname: String,
    ),
  })
end

class WorkerConfig
  YAML.mapping({
    db: NamedTuple(
      user: String,
      password: String,
      host: String,
      port: Int32,
      dbname: String,
    ),
    access_key: String,
    secret_key: String,
    batch_url:  String,
    batch_size: Int32?,
  })
end
