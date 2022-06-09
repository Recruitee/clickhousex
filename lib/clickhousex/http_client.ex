defmodule Clickhousex.HTTPClient do
  alias Clickhousex.Query
  @moduledoc false

  @codec Application.get_env(:clickhousex, :codec, Clickhousex.Codec.JSON)

  @req_headers [{"Content-Type", "text/plain"}]

  def send(query, request, base_address, timeout, nil, _password, database) do
    send_p(query, request, base_address, database, timeout: timeout, recv_timeout: timeout)
  end

  def send(query, request, base_address, timeout, username, password, database) do
    opts = [hackney: [basic_auth: {username, password}], timeout: timeout, recv_timeout: timeout]
    send_p(query, request, base_address, database, opts)
  end

  defp send_p(query, request, base_address, database, opts) do
    command = parse_command(query)
    url_size = calculate_url_size(base_address, database, request)

    post_body = build_post_body(query, request, url_size)
    http_opts = build_http_opts(opts, database, request, url_size)

    with {:ok, %{status_code: 200, body: body}} <-
           HTTPoison.post(base_address, post_body, @req_headers, http_opts),
         {:command, :selected} <- {:command, command},
         {:ok, %{column_names: column_names, rows: rows}} <- @codec.decode(body) do
      {:ok, command, column_names, rows}
    else
      {:command, :updated} -> {:ok, :updated, 1}
      {:ok, response} -> {:error, response.body}
      {:error, error} -> {:error, error.reason}
    end
  end

  defp calculate_url_size(base_address, database, request) do
    query_string_data = IO.iodata_to_binary(request.query_string_data)

    url =
      base_address <>
        "?" <> URI.encode_www_form("database=#{database}" <> "&query=#{query_string_data}")

    byte_size(url)
  end

  defp parse_command(%Query{type: :select}), do: :selected
  defp parse_command(_), do: :updated

  defp build_post_body(_, request, url_size) when url_size >= 16384 do
    request.query_string_data
    |> IO.iodata_to_binary()
    |> append_format()
  end

  defp build_post_body(query, request, _) do
    maybe_append_format(query, request)
  end

  defp build_http_opts(opts, database, request, url_size) when url_size >= 16384 do
    Keyword.put(opts, :params, %{database: database, query: ""})
  end

  defp build_http_opts(opts, database, request, _) do
    Keyword.put(opts, :params, %{
      database: database,
      query: IO.iodata_to_binary(request.query_string_data)
    })
  end

  defp maybe_append_format(%Query{type: :select}, request) do
    append_format(request.post_data)
  end

  defp maybe_append_format(_, request) do
    [request.post_data]
  end

  defp append_format(data) do
    [data, " FORMAT ", @codec.response_format()]
  end
end
