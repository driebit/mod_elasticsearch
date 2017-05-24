-record(elasticsearch_put, {
    index :: binary(),
    type :: binary(),
    id :: binary()
}).

-record(elasticsearch_fields, {
    query :: binary() | map()
}).
