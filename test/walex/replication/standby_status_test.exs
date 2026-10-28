defmodule WalEx.Replication.StandbyStatusTest do
  use ExUnit.Case, async: false

  alias WalEx.Config
  alias WalEx.Replication.{Server, WalTracker}

  @hostname System.get_env("PGHOST", "localhost")
  @username System.get_env("PGUSER", "postgres")
  @password System.get_env("PGPASSWORD", "postgres")
  @port String.to_integer(System.get_env("PGPORT", "5432"))

  @base_db [
    hostname: @hostname,
    username: @username,
    password: @password,
    database: "postgres",
    port: @port,
    ssl: false,
    ssl_opts: [verify: :verify_none]
  ]

  setup do
    start_supervised!(WalEx.Config.Registry.child_spec())
    :ok
  end

  defp start_config!(app, extra) do
    configs =
      [name: app, publication: "publication", subscriptions: ["subscriptions"]] ++
        @base_db ++ extra

    {:ok, _pid} = Config.start_link(configs: configs)
  end

  defp state(app, extra \\ []) do
    Map.merge(
      %{app_name: app, streaming?: true, last_wal_end: 0, in_txn: false, standby_timer: nil},
      Map.new(extra)
    )
  end

  defp keepalive(wal_end, reply), do: <<?k, wal_end::64, 0::64, reply::8>>
  defp xlog(wal_end, payload), do: <<?w, 0::64, wal_end::64, 0::64, payload::binary>>
  defp ack_lsn([<<?r, lsn::64, _::64, _::64, _::64, _::8>>]), do: lsn

  describe "config" do
    test "standby_status_interval defaults to disabled" do
      start_config!(:sb_default, [])
      assert Config.get_configs(:sb_default, :standby_status_interval) == nil
    end

    test "standby_status_interval accepts a positive integer" do
      start_config!(:sb_set, standby_status_interval: 5_000)
      assert Config.get_configs(:sb_set, :standby_status_interval) == 5_000
    end

    test "standby_status_interval rejects anything else" do
      for bad <- [0, -1, "5s", 1.5] do
        assert_raise RuntimeError, ~r/standby_status_interval/, fn ->
          start_config!(:sb_bad, standby_status_interval: bad)
        end
      end
    end
  end

  describe "tracking the WAL position" do
    test "a keepalive without a reply request records wal_end" do
      app = :sb_track_k
      start_config!(app, [])

      assert {:noreply, [], %{last_wal_end: 700}} =
               Server.handle_data(keepalive(700, 0), state(app))
    end

    test "the position never moves backwards" do
      app = :sb_track_back
      start_config!(app, [])

      assert {:noreply, [], %{last_wal_end: 900}} =
               Server.handle_data(keepalive(700, 0), state(app, last_wal_end: 900))
    end
  end

  describe "periodic standby status" do
    test "does not arm a timer when no interval is configured" do
      app = :sb_off
      start_config!(app, [])

      assert {:noreply, _msgs, %{standby_timer: nil}} =
               Server.handle_info(:standby_status, state(app, last_wal_end: 100))
    end

    test "sends wal_end + 1 and reschedules while streaming" do
      app = :sb_send
      start_config!(app, standby_status_interval: 60_000)

      assert {:noreply, msgs, %{standby_timer: ref}} =
               Server.handle_info(:standby_status, state(app, last_wal_end: 100))

      assert ack_lsn(msgs) == 101
      assert is_reference(ref)
      Process.cancel_timer(ref)
    end

    test "sends nothing before any WAL position is known, but keeps ticking" do
      app = :sb_unknown
      start_config!(app, standby_status_interval: 60_000)

      assert {:noreply, [], %{standby_timer: ref}} =
               Server.handle_info(:standby_status, state(app))

      assert is_reference(ref)
      Process.cancel_timer(ref)
    end

    test "stops ticking when replication is not streaming" do
      app = :sb_stopped
      start_config!(app, standby_status_interval: 60_000)

      assert {:noreply, %{standby_timer: nil}} =
               Server.handle_info(
                 :standby_status,
                 state(app, streaming?: false, last_wal_end: 5)
               )
    end
  end

  describe "with ack_after_processing" do
    test "an idle connection advances to the server's position" do
      app = :sb_ack_idle

      start_config!(app,
        durable_slot: true,
        ack_after_processing: true,
        standby_status_interval: 60_000
      )

      start_supervised!({WalTracker, app_name: app})
      WalTracker.update(app, 50)

      assert {:noreply, msgs, %{standby_timer: ref}} =
               Server.handle_info(:standby_status, state(app, last_wal_end: 5_000))

      assert ack_lsn(msgs) == 5_001
      Process.cancel_timer(ref)
    end

    test "an open transaction holds the ack at the confirmed position" do
      app = :sb_ack_txn

      start_config!(app,
        durable_slot: true,
        ack_after_processing: true,
        standby_status_interval: 60_000
      )

      start_supervised!({WalTracker, app_name: app})
      WalTracker.update(app, 50)

      assert {:noreply, msgs, %{standby_timer: ref}} =
               Server.handle_info(
                 :standby_status,
                 state(app, last_wal_end: 5_000, in_txn: true)
               )

      assert ack_lsn(msgs) == 51
      Process.cancel_timer(ref)
    end

    test "a keepalive reply request also advances when idle" do
      app = :sb_ack_keepalive
      start_config!(app, durable_slot: true, ack_after_processing: true)
      start_supervised!({WalTracker, app_name: app})
      WalTracker.update(app, 50)

      assert {:noreply, msgs, _} = Server.handle_data(keepalive(8_000, 1), state(app))
      assert ack_lsn(msgs) == 8_001
    end

    test "BEGIN marks a transaction open and COMMIT closes it" do
      app = :sb_ack_flag
      start_config!(app, durable_slot: true, ack_after_processing: true)
      start_supervised!({WalTracker, app_name: app})
      message_middleware = fn _msg, _app -> :ok end
      st = state(app, message_middleware: message_middleware)

      begin = <<?B, 0::64, 0::64, 0::32>>
      commit = <<?C, 0::8, 0::64, 0::64, 0::64>>

      assert {:noreply, %{in_txn: true}} = Server.handle_data(xlog(10, begin), st)

      assert {:noreply, %{in_txn: false}} =
               Server.handle_data(xlog(20, commit), %{st | in_txn: true})
    end
  end
end
