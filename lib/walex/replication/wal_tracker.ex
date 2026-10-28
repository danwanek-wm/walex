defmodule WalEx.Replication.WalTracker do
  @moduledoc """
  Tracks the last successfully processed WAL LSN (Log Sequence Number).

  When `ack_after_processing: true` is configured alongside `durable_slot: true`,
  this agent holds the most recently confirmed LSN so that the replication server
  only acknowledges positions that have been fully processed by all event handlers.

  This prevents Postgres from discarding WAL segments for transactions whose
  handlers have not yet completed, giving permanent-slot users at-least-once
  delivery semantics on reconnect.

  ## LSN representation

  LSNs are stored as raw 64-bit integers matching the wire format used by
  Postgrex.ReplicationConnection. The value `0` is the sentinel meaning
  "nothing confirmed yet"; in that case the replication server falls back to
  acknowledging the position reported by Postgres in the keepalive message.
  """

  use Agent

  alias WalEx.Config.Registry, as: WalExRegistry

  @unconfirmed 0

  @doc """
  Starts the WalTracker agent for the given `app_name`.
  """
  @spec start_link(keyword()) :: Agent.on_start()
  def start_link(opts) do
    app_name = Keyword.get(opts, :app_name)
    name = WalExRegistry.set_name(:set_agent, __MODULE__, app_name)

    Agent.start_link(fn -> @unconfirmed end, name: name)
  end

  @doc """
  Returns the last confirmed LSN for `app_name`.

  Returns `0` when no transaction has been confirmed yet.
  """
  @spec get(atom()) :: non_neg_integer()
  def get(app_name) do
    Agent.get(agent_name(app_name), & &1)
  end

  @doc """
  Updates the confirmed LSN for `app_name`.

  The new position is only stored when it is strictly greater than the
  current value, ensuring the LSN never moves backward.
  """
  @spec update(atom(), non_neg_integer()) :: :ok
  def update(app_name, lsn) when is_integer(lsn) and lsn > 0 do
    Agent.update(agent_name(app_name), fn current ->
      if lsn > current, do: lsn, else: current
    end)
  end

  def update(_app_name, _lsn), do: :ok

  @doc """
  Returns `true` when no LSN has been confirmed yet.
  """
  @spec empty?(atom()) :: boolean()
  def empty?(app_name) do
    get(app_name) == @unconfirmed
  end

  defp agent_name(app_name) do
    WalExRegistry.set_name(:set_agent, __MODULE__, app_name)
  end
end
