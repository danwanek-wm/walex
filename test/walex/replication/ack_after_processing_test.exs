defmodule WalEx.Replication.AckAfterProcessingTest do
  use ExUnit.Case, async: false

  alias WalEx.Config
  alias WalEx.Decoder.Messages
  alias WalEx.Events
  alias WalEx.Events.EventModules
  alias WalEx.Replication.{Publisher, Server, WalTracker}

  @base_db [
    hostname: "localhost",
    username: "postgres",
    password: "postgres",
    database: "postgres",
    port: 5432,
    ssl: false,
    ssl_opts: [verify: :verify_none]
  ]

  setup do
    start_supervised!(WalEx.Config.Registry.child_spec())
    :ok
  end

  defp app_name(suffix), do: String.to_atom("ack_after_processing_test_#{suffix}")

  defp start_config!(app_name, extra) do
    configs =
      [
        name: app_name,
        publication: "publication",
        subscriptions: ["subscriptions"]
      ] ++ @base_db ++ extra

    {:ok, _pid} = Config.start_link(configs: configs)
  end

  defp keepalive(wal_end, reply), do: <<?k, wal_end::64, 0::64, reply::8>>

  defp extract_ack_lsn([<<?r, write_lsn::64, _flush::64, _apply::64, _ts::64, _reply::8>>]),
    do: write_lsn

  # Mirrors the encoding in Publisher.maybe_confirm_lsn/2
  defp encode_lsn({xlog_file, xlog_offset}), do: Bitwise.bsl(xlog_file, 32) + xlog_offset

  describe "Server keepalive acknowledgements" do
    test "with ack_after_processing true and no confirmed txn, falls back to wal_end + 1" do
      app = app_name("server_fallback")

      start_config!(app, durable_slot: true, ack_after_processing: true)
      start_supervised!({WalTracker, app_name: app})

      state = %{
        app_name: app,
        streaming?: false,
        standby_timer: nil,
        ack_after_processing: Config.get_configs(app, :ack_after_processing),
        standby_status_interval: nil,
        slot_confirmed_lsn: nil,
        last_wal_end: 0,
        in_txn: false
      }

      assert {:noreply, messages, %{app_name: ^app}} =
               Server.handle_data(keepalive(1_000, 1), state)

      assert 1_001 == extract_ack_lsn(messages)
    end

    test "with ack_after_processing true and confirmed txn, reports confirmed_lsn + 1" do
      app = app_name("server_confirmed")

      start_config!(app, durable_slot: true, ack_after_processing: true)
      start_supervised!({WalTracker, app_name: app})
      WalTracker.update(app, 2_000)

      # A transaction is open, so the server's position is not safe to ack.
      state = %{
        app_name: app,
        streaming?: false,
        standby_timer: nil,
        ack_after_processing: Config.get_configs(app, :ack_after_processing),
        standby_status_interval: nil,
        slot_confirmed_lsn: nil,
        last_wal_end: 0,
        in_txn: true
      }

      # wal_end is intentionally larger than the confirmed tracker position;
      # ACK must be driven by the tracker while a transaction is open.
      assert {:noreply, messages, %{app_name: ^app}} =
               Server.handle_data(keepalive(9_999, 1), state)

      assert 2_001 == extract_ack_lsn(messages)
    end

    test "with ack_after_processing false, keeps default wal_end + 1 behaviour" do
      app = app_name("server_disabled")

      start_config!(app, durable_slot: true, ack_after_processing: false)
      start_supervised!({WalTracker, app_name: app})
      # Confirmed tracker value must be ignored when feature is disabled
      WalTracker.update(app, 42)

      state = %{
        app_name: app,
        streaming?: false,
        standby_timer: nil,
        ack_after_processing: Config.get_configs(app, :ack_after_processing),
        standby_status_interval: nil,
        slot_confirmed_lsn: nil,
        last_wal_end: 0,
        in_txn: false
      }

      assert {:noreply, messages, %{app_name: ^app}} =
               Server.handle_data(keepalive(5_000, 1), state)

      assert 5_001 == extract_ack_lsn(messages)
    end

    test "does not send a status update when keepalive reply flag is 0" do
      app = app_name("server_no_reply")

      start_config!(app, durable_slot: true, ack_after_processing: true)
      start_supervised!({WalTracker, app_name: app})
      WalTracker.update(app, 123)

      state = %{
        app_name: app,
        streaming?: false,
        standby_timer: nil,
        ack_after_processing: Config.get_configs(app, :ack_after_processing),
        standby_status_interval: nil,
        slot_confirmed_lsn: nil,
        last_wal_end: 0,
        in_txn: false
      }

      assert {:noreply, [], %{app_name: ^app}} = Server.handle_data(keepalive(9_999, 0), state)
    end
  end

  describe "liveness while a transaction is open and nothing is confirmed" do
    defp open_txn_state(app, extra \\ []) do
      Map.merge(
        %{
          app_name: app,
          step: :streaming,
          slot_name: "ack_slot",
          publication: "publication",
          streaming?: true,
          standby_timer: nil,
          ack_after_processing: true,
          standby_status_interval: nil,
          slot_confirmed_lsn: nil,
          last_wal_end: 0,
          in_txn: true
        },
        Map.new(extra)
      )
    end

    setup do
      app = app_name("liveness_#{System.unique_integer([:positive])}")
      start_config!(app, durable_slot: true, ack_after_processing: true)
      start_supervised!({WalTracker, app_name: app})
      %{app: app}
    end

    test "answers a reply request with LSN 0 when the slot position is unknown", %{app: app} do
      assert {:noreply, messages, _state} =
               Server.handle_data(keepalive(9_999, 1), open_txn_state(app))

      assert 0 == extract_ack_lsn(messages)
    end

    test "answers a reply request with the slot's confirmed_flush_lsn when known", %{app: app} do
      assert {:noreply, messages, _state} =
               Server.handle_data(
                 keepalive(9_999, 1),
                 open_txn_state(app, slot_confirmed_lsn: 4_000)
               )

      assert 4_000 == extract_ack_lsn(messages)
    end

    test "never reports below the slot's confirmed_flush_lsn", %{app: app} do
      WalTracker.update(app, 100)

      assert {:noreply, messages, _state} =
               Server.handle_data(
                 keepalive(9_999, 1),
                 open_txn_state(app, slot_confirmed_lsn: 4_000)
               )

      assert 4_000 == extract_ack_lsn(messages)
    end

    test "sends no unsolicited periodic update when nothing is safe to report", %{app: app} do
      assert {:noreply, [], _state} =
               Server.handle_info(:standby_status, open_txn_state(app, last_wal_end: 9_999))
    end
  end

  describe "reading the slot position at connect" do
    defp slot_state(app, step) do
      %{
        app_name: app,
        step: step,
        slot_name: "ack_slot",
        publication: "publication",
        durable_slot: true,
        slot_confirmed_lsn: nil
      }
    end

    defp slot_row(active, confirmed) do
      [
        %Postgrex.Result{
          columns: ["active", "confirmed_flush_lsn"],
          rows: [[active, confirmed]],
          num_rows: 1
        }
      ]
    end

    test "an existing inactive slot records confirmed_flush_lsn and starts replication" do
      app = app_name("slot_inactive")

      assert {:stream, _query, [], %{slot_confirmed_lsn: lsn}} =
               Server.handle_result(slot_row("f", "16/B374D848"), slot_state(app, :slot_exists))

      assert lsn == 0x16 * 0x1_0000_0000 + 0xB374D848
    end

    test "an existing active slot records confirmed_flush_lsn while it waits" do
      app = app_name("slot_active")

      assert {:noreply, %{slot_confirmed_lsn: 0x2A}} =
               Server.handle_result(slot_row("t", "0/2A"), slot_state(app, :slot_exists))
    end

    test "a NULL or malformed confirmed_flush_lsn is treated as unknown" do
      app = app_name("slot_null")

      for value <- [nil, "garbage", "0/0"] do
        assert {:stream, _query, [], %{slot_confirmed_lsn: nil}} =
                 Server.handle_result(slot_row("f", value), slot_state(app, :slot_exists))
      end
    end

    test "a newly created slot records consistent_point" do
      app = app_name("slot_created")

      result = %Postgrex.Result{
        columns: ["slot_name", "consistent_point", "snapshot_name", "output_plugin"],
        rows: [["ack_slot", "1/10", nil, "pgoutput"]],
        num_rows: 1
      }

      assert {:stream, _query, [], %{slot_confirmed_lsn: lsn}} =
               Server.handle_result([result], slot_state(app, :create_slot))

      assert lsn == 0x1_0000_0010
    end
  end

  describe "Publisher confirmation boundary" do
    test "LSN is not confirmed until the matching COMMIT is processed" do
      app = app_name("publisher_commit_boundary")

      start_config!(app, durable_slot: true, ack_after_processing: true)
      start_supervised!({WalTracker, app_name: app})
      start_supervised!({EventModules, app_name: app})
      start_supervised!({Events, app_name: app})
      start_supervised!({Publisher, app_name: app})

      ts = ~U[2025-01-01 00:00:00Z]
      lsn = {0, 123}
      other_lsn = {0, 999}

      begin_msg = %Messages.Begin{final_lsn: lsn, commit_timestamp: ts, xid: 1}

      commit_same_lsn = %Messages.Commit{
        flags: [],
        lsn: lsn,
        end_lsn: lsn,
        commit_timestamp: ts
      }

      commit_other_lsn = %Messages.Commit{
        flags: [],
        lsn: other_lsn,
        end_lsn: other_lsn,
        commit_timestamp: ts
      }

      # Nothing confirmed before any messages
      assert 0 == WalTracker.get(app)

      # BEGIN alone must not advance the tracker
      :ok = Publisher.process_message_sync(begin_msg, app)
      assert 0 == WalTracker.get(app)

      # A COMMIT whose LSN does not match the open transaction must not advance the tracker
      :ok = Publisher.process_message_sync(commit_other_lsn, app)
      assert 0 == WalTracker.get(app)

      # The matching COMMIT must advance the tracker to the encoded LSN
      :ok = Publisher.process_message_sync(commit_same_lsn, app)
      assert encode_lsn(lsn) == WalTracker.get(app)
    end

    test "when ack_after_processing is disabled, publisher does not update the wal tracker" do
      app = app_name("publisher_disabled")

      start_config!(app, durable_slot: true, ack_after_processing: false)
      start_supervised!({WalTracker, app_name: app})
      start_supervised!({EventModules, app_name: app})
      start_supervised!({Events, app_name: app})
      start_supervised!({Publisher, app_name: app})

      ts = ~U[2025-01-01 00:00:00Z]
      lsn = {1, 1}

      :ok =
        Publisher.process_message_sync(
          %Messages.Begin{final_lsn: lsn, commit_timestamp: ts, xid: 99},
          app
        )

      :ok =
        Publisher.process_message_sync(
          %Messages.Commit{flags: [], lsn: lsn, end_lsn: lsn, commit_timestamp: ts},
          app
        )

      assert 0 == WalTracker.get(app)
    end
  end
end
