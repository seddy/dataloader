use Mix.Config

config :dataloader, Dataloader.TestRepo,
  hostname: "localhost",
  database: "dataloader_test",
  adapter: Ecto.Adapters.Postgres,
  pool: Ecto.Adapters.SQL.Sandbox,
  username: "nested",
  password: "nested",
  port: 54321

config :dataloader, ecto_repos: [Dataloader.TestRepo]

config :logger, level: :warn
