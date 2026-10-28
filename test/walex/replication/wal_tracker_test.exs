defmodule WalEx.Replication.WalTrackerTest do
  use ExUnit.Case, async: false

  alias WalEx.Replication.WalTracker

  @app_name :wal_tracker_test_app

  setup do
    start_supervised!({Registry, keys: :unique, name: :walex_registry})
    start_supervised!({WalTracker, app_name: @app_name})
    :ok
  end

  describe "get/1" do
    test "returns 0 when nothing has been confirmed yet" do
      assert 0 == WalTracker.get(@app_name)
    end
  end

  describe "empty?/1" do
    test "returns true before any LSN is confirmed" do
      assert WalTracker.empty?(@app_name)
    end

    test "returns false after an LSN is confirmed" do
      WalTracker.update(@app_name, 100)
      refute WalTracker.empty?(@app_name)
    end
  end

  describe "update/2" do
    test "stores the given LSN" do
      WalTracker.update(@app_name, 500)
      assert 500 == WalTracker.get(@app_name)
    end

    test "advances the LSN when a higher value is provided" do
      WalTracker.update(@app_name, 100)
      WalTracker.update(@app_name, 200)
      assert 200 == WalTracker.get(@app_name)
    end

    test "does not move the LSN backward" do
      WalTracker.update(@app_name, 300)
      WalTracker.update(@app_name, 100)
      assert 300 == WalTracker.get(@app_name)
    end

    test "is idempotent when the same LSN is provided twice" do
      WalTracker.update(@app_name, 200)
      WalTracker.update(@app_name, 200)
      assert 200 == WalTracker.get(@app_name)
    end

    test "ignores zero" do
      WalTracker.update(@app_name, 0)
      assert 0 == WalTracker.get(@app_name)
    end

    test "ignores negative values" do
      WalTracker.update(@app_name, -1)
      assert 0 == WalTracker.get(@app_name)
    end

    test "does not affect a separate app_name" do
      other_app = :wal_tracker_other_app
      start_supervised!({WalTracker, app_name: other_app}, id: :other_tracker)

      WalTracker.update(@app_name, 999)

      assert 0 == WalTracker.get(other_app)
    end
  end
end
