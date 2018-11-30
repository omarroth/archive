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
