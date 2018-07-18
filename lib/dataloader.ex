defmodule Dataloader do
  defmodule GetError do
    defexception message:
                   "Failed to get data, this may mean it has not been loaded, see Dataloader documentation for more info."
  end

  @moduledoc """
  # Dataloader

  Dataloader provides an easy way efficiently load data in batches. It's
  inspired by https://github.com/facebook/dataloader, although it makes some
  small API changes to better suit Elixir use cases.

  Central to Dataloader is the idea of a source. A single Dataloader struct can
  have many different sources, which represent different ways to load data.

  Here's an example of a data loader using an ecto source, and then loading some
  organization data.

  ```elixir
  source = Dataloader.Ecto.new(MyApp.Repo)

  # setup the loader
  loader = Dataloader.new |> Dataloader.add_source(:db, source)

  # load some things
  loader =
    loader
    |> Dataloader.load(:db, Organization, 1)
    |> Dataloader.load_many(:db, Organization, [4, 9])

  # actually retrieve them
  loader = Dataloader.run(loader)

  # Now we can get whatever values out we want
  organizations = Dataloader.get_many(loader, :db, Organization, [1,4])
  ```

  This will do a single SQL query to get all organizations by ids 1,4, and 9.
  You can load multiple batches from multiple sources, and then when `run/1` is
  called batch will be loaded concurrently.

  Here we named the source `:db` within our dataloader. More commonly though if
  you're using Phoenix you'll want to name it after one of your contexts, and
  have a different source used for each context. This provides an easy way to
  enforce data access rules within each context. See the `DataLoader.Ecto`
  moduledocs for more details
  """
  defstruct sources: %{},
            options: []

  require Logger
  alias Dataloader.Source

  @type t :: %__MODULE__{
          sources: %{source_name => Dataloader.Source.t()},
          options: [option]
        }

  @type option :: {:timeout, pos_integer}
  @type source_name :: any

  @default_timeout 15_000
  def default_timeout, do: @default_timeout

  @spec new([option]) :: t
  def new(opts \\ []), do: %__MODULE__{options: opts}

  @spec add_source(t, source_name, Dataloader.Source.t()) :: t
  def add_source(%{sources: sources} = loader, name, source) do
    sources = Map.put(sources, name, source)
    %{loader | sources: sources}
  end

  @spec load_many(t, source_name, any, [any]) :: t | no_return()
  def load_many(loader, source_name, batch_key, vals) when is_list(vals) do
    source =
      loader
      |> get_source(source_name)
      |> do_load(batch_key, vals)

    put_in(loader.sources[source_name], source)
  end

  @spec load(t, source_name, any, any) :: t | no_return()
  def load(loader, source_name, batch_key, val) do
    load_many(loader, source_name, batch_key, [val])
  end

  defp do_load(source, batch_key, vals) do
    Enum.reduce(vals, source, &Source.load(&2, batch_key, &1))
  end

  @spec run(t) :: t | no_return
  def run(dataloader) do
    if pending_batches?(dataloader) do
      fun = fn {name, source} -> {name, Source.run(source)} end

      sources =
        async_safely(__MODULE__, :run_tasks, [
          dataloader.sources,
          fun,
          [timeout: dataloader_timeout(dataloader)]
        ])
        |> Enum.map(fn
          {_source, {:ok, {name, source}}} -> {name, source}
          {_source, {:error, reason}} -> {:error, reason}
        end)
        |> Map.new()

      %{dataloader | sources: sources}
    else
      dataloader
    end
  end

  defp dataloader_timeout(dataloader) do
    max_source_timeout =
      dataloader.sources
      |> Enum.map(fn {_, source} -> Source.timeout(source) end)
      |> Enum.max()

    (max_source_timeout || @default_timeout) + :timer.seconds(1)
  end

  @spec get(t, source_name, any, any) :: any | no_return()
  def get(loader, source, batch_key, item_key) do
    loader
    |> get_source(source)
    |> Source.fetch(batch_key, item_key)
    |> do_get
  end

  defp do_get({:ok, val}), do: val
  defp do_get({:error, reason}), do: raise(Dataloader.GetError, inspect(reason))

  @spec get_many(t, source_name, any, any) :: [any] | no_return()
  def get_many(loader, source, batch_key, item_keys) when is_list(item_keys) do
    source = get_source(loader, source)

    for key <- item_keys do
      source
      |> Source.fetch(batch_key, key)
      |> do_get
    end
  end

  def put(loader, source_name, batch_key, item_key, result) do
    source =
      loader
      |> get_source(source_name)
      |> Source.put(batch_key, item_key, result)

    put_in(loader.sources[source_name], source)
  end

  @spec pending_batches?(t) :: boolean
  def pending_batches?(loader) do
    Enum.any?(loader.sources, fn {_name, source} -> Source.pending_batches?(source) end)
  end

  defp get_source(loader, source_name) do
    loader.sources[source_name] || raise "Source does not exist: #{inspect(source_name)}"
  end

  @doc """
  This is a helper method to run a set of async tasks in a separate supervision
  tree which:

  1. Is run by a supervisor linked to the main process. This ensures any async
     tasks will get killed if the main process is killed.
  2. Spawns a separate task which traps exits for running the provided
     function. This ensures we will always have some output, but are not calling
     setting `:trap_exit` on the main process.

  **NOTE**: The provided `fun` must accept a `Task.Supervisor` as its first
  argument, as this prepends the relevant supervisor to `args`

  See `run_tasks/4` for an example of a `fun` implementation, this will return
  whatever that returns.
  """
  @spec async_safely(module(), fun(), keyword()) :: any()
  def async_safely(mod, fun, args \\ []) do
    # This supervisor exists to help ensure that the spawned tasks will die as
    # promptly as possible if the current process is killed.
    {:ok, task_supervisor} = Task.Supervisor.start_link([])
    args = [task_supervisor | args]

    # The intermediary task is spawned here so that the `:trap_exit` flag does
    # not lead to rogue behaviour within the current process. This could happen
    # if the current process is linked to something, and then that something
    # dies in the middle of us loading stuff.
    task =
      Task.async(fn ->
        # The purpose of `:trap_exit` here is so that we can ensure that any failures
        # within the tasks do not kill the current process. We want to get results
        # back no matter what.
        Process.flag(:trap_exit, true)

        apply(mod, fun, args)
      end)

    # The infinity is safe here because the internal
    # tasks all have their own timeout.
    Task.await(task, :infinity)
  end

  @doc ~S"""
  This helper function will call `fun`on all `items` asynchronously, returning
  a map of `:ok`/`:error` tuples, keyed off the `items`. For example:

      iex> {:ok, task_supervisor} = Task.Supervisor.start_link([])
      ...> Dataloader.run_tasks(task_supervisor, [1,2,3], fn x -> x * x end, [])
      %{
        1 => {:ok, 1},
        2 => {:ok, 4},
        3 => {:ok, 9}
      }

  Similarly, for errors:

      iex> {:ok, task_supervisor} = Task.Supervisor.start_link([])
      ...> Dataloader.run_tasks(task_supervisor, [1,2,3], fn _x -> Process.sleep(5) end, [timeout: 1])
      %{
        1 => {:error, :timeout},
        2 => {:error, :timeout},
        3 => {:error, :timeout}
      }
  """
  @spec run_tasks(Task.Supervisor.t(), list(), fun(), keyword()) :: map()
  def run_tasks(task_supervisor, items, fun, opts \\ []) do
    task_opts = [
      timeout: opts[:timeout] || @default_timeout,
      on_timeout: :kill_task
    ]

    results =
      task_supervisor
      |> Task.Supervisor.async_stream(items, fun, task_opts)
      |> Enum.map(fn
        {:ok, result} -> {:ok, result}
        {:exit, reason} -> {:error, reason}
      end)

    Enum.zip(items, results)
    |> Map.new()
  end

  @doc """
  ## DEPRECATED

  This function has been deprecated in favour of `async_safely/3`. This used to
  be used by both the `Dataloader` module for running multiple source queries
  concurrently, and the `KV` and `Ecto` sources to actually run separate batch
  fetches (e.g. for `Posts` and `Users` at the same time).

  The problem was that the behaviour between the sources and the parent
  `Dataloader` was actually slightly different. The `Dataloader`-specific
  behaviour has been pulled out into `run_tasks/4`

  Please use `async_safely` instead of this for fetching data from sources
  """
  @spec pmap(list(), fun(), keyword()) :: map()
  def pmap(items, fun, opts \\ []) do
    async_safely(__MODULE__, :run_tasks, [items, fun, opts])
  end
end
