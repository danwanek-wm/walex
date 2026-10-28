{:ok, _} = Application.ensure_all_started(:postgrex)
ExUnit.start(capture_log: true)
