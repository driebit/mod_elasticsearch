-record(elasticsearch_put, {
    index :: binary(),
    type :: binary(),
    id :: binary()
}).

-record(elasticsearch_fields, {
    query :: binary() | map()
}).

%% @doc Search options
%%      fallback: whether to fall back to PostgreSQL for non full-text searches.
-record(elasticsearch_options, {
    fallback = true :: boolean()
}).
