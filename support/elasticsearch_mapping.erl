%% Elasticsearch mapping
-module(elasticsearch_mapping).
-author("Driebit <tech@driebit.nl>").

-include("zotonic.hrl").

-export([
    map_rsc/2,
    default_mapping/2,
    hash/1,
    dynamic_language_mapping/1,
    default_translation/2
]).

%% Map a Zotonic resource
-spec map_rsc(m_rsc:rid(), z:context()) -> proplists:proplist().
map_rsc(Id, Context) ->
    Props = m_rsc:get(Id, Context),
    [
        {category, m_rsc:is_a(Id, Context)},
        {pivot_title, default_translation(proplists:get_value(title, Props), Context)}
    ] ++
    map_location(Id, Context) ++
    map_properties(Props) ++
    map_edges(Id, Context).

map_location(Id, Context) ->
    case {m_rsc:p(Id, location_lat, Context), m_rsc:p(Id, location_lng, Context)} of
        {_, undefined} ->
            [];
        {undefined, _} ->
            [];
        {Lat, Lng} ->
            [{geolocation, [
                {lat, z_convert:to_float(Lat)},
                {lon, z_convert:to_float(Lng)}
            ]}]
    end.

map_properties(Properties) ->
    lists:foldl(
        fun(Prop, Acc) ->
            case map_property(Prop) of
                undefined ->
                    Acc;
                {Key, Value} ->
                    [{Key, Value}|Acc];
                Values when is_list(Values) ->
                    %% Property was split into multiple properties
                    Values ++ Acc
            end
        end,
        [],
        without_ignored(Properties)
    ).

without_ignored(Properties) ->
    lists:filter(
        fun({Key, _Value}) ->
            not lists:member(Key, ignored_properties())
        end,
        Properties
    ).

map_property({Key, {trans, Translations}}) ->
    lists:map(
        fun({LangCode, Translation}) ->
            %% Change keys to contain language code: title_en etc.
            LangKey = get_language_property(Key, LangCode),

            % Strip HTML tags from translated props. This is
            % only needed for body, but won't hurt the other
            % translated props.
            {LangKey, z_html:strip(Translation)}
        end,
        Translations
    );
map_property({blocks, Blocks}) ->
    {blocks, lists:map(
        fun map_properties/1,
        Blocks
    )};
map_property({Key, Value}) ->
    {Key, map_value(Value)}.

% Make Zotonic resource value Elasticsearch-friendly
map_value({{Y, M, D}, {H, I, S}} = DateTime) when
    is_integer(Y), is_integer(M), is_integer(D),
    is_integer(H), is_integer(I), is_integer(S) ->
    case date_to_iso8601(DateTime) of
        undefined ->
            %% e.g., ?ST_JUTTEMIS or invalid dates
            null;
        Value ->
            z_convert:to_binary(Value)
    end;
map_value({{_, _, _}, {_, _, _}}) ->
    %% Invalid date
    null;
map_value(undefined) ->
    null;
map_value(Value) ->
    Value.

map_edges(Id, Context) ->
    [
        {incoming_edges, incoming_edges(Id, Context)},
        {outgoing_edges, outgoing_edges(Id, Context)}
    ].

incoming_edges(Id, Context) ->
    lists:flatmap(
        fun(Predicate) ->
            lists:map(fun map_edge/1, m_edge:subject_edge_props(Id, Predicate, Context))
        end,
        m_edge:subject_predicates(Id, Context)
    ).

outgoing_edges(Id, Context) ->
    lists:flatmap(
        fun(Predicate) ->
            lists:map(fun map_edge/1, m_edge:object_edge_props(Id, Predicate, Context))
        end,
        m_edge:object_predicates(Id, Context)
    ).

map_edge(Edge) ->
    [
        {predicate_id, proplists:get_value(predicate_id, Edge)},
        {subject_id, proplists:get_value(subject_id, Edge)},
        {object_id, proplists:get_value(object_id, Edge)},
        {created, map_value(proplists:get_value(created, Edge))}
    ].

%% Get a default Elasticsearch mapping for Zotonic resources
-spec default_mapping(atom(), z:context()) -> {Hash :: binary(), Mapping :: map()}.
default_mapping(resource, Context) ->
    Mapping = #{
        <<"properties">> => #{
            <<"geolocation">> => #{
                <<"type">> => <<"geo_point">>
            },
            <<"blocks">> => #{
                <<"type">> => <<"nested">>
            },
            <<"incoming_edges">> => #{
                <<"type">> => <<"nested">>
            },
            <<"outgoing_edges">> => #{
                <<"type">> => <<"nested">>
            },
            <<"date_start">> => #{
                <<"type">> => <<"date">>
            },
            <<"date_end">> => #{
                <<"type">> => <<"date">>
            },
            <<"suggest">> => #{
                <<"type">> => <<"completion">>,
                <<"analyzer">> =>  <<"simple">>
            },
            <<"pivot_title">> => #{
                <<"type">> => <<"keyword">>
            }
        },
        <<"dynamic_templates">> => dynamic_language_mapping(Context)
    },
    {hash(Mapping), Mapping}.

%% @doc Generate unique SHA1-based hash for a mapping
-spec hash(map()) -> binary().
hash(Map) ->
    z_string:to_lower(z_utils:hex_encode(crypto:hash(sha, jiffy:encode(Map)))).

%% @doc Get dynamic language mappings, based on available languages.
-spec dynamic_language_mapping(z:context()) -> list().
dynamic_language_mapping(Context) ->
    lists:map(
        fun({LangCode, _}) ->
            #{LangCode => #{
                <<"match">> => <<"*_", (z_convert:to_binary(LangCode))/binary>>,
                <<"match_mapping_type">> => <<"string">>,
                <<"mapping">> => #{
                    <<"type">> => <<"text">>,
                    <<"analyzer">> => get_analyzer(LangCode)
                }
            }}
        end,
        m_translation:language_list_enabled(Context)
    ).

%% Get analyzer for a language, depending on which languages are supported by
%% Elasticsearch
get_analyzer(LangCode) ->
    Language = string:to_lower(
        binary_to_list(
            iso639:lc2lang(
                atom_to_list(LangCode)
            )
        )
    ),

    case is_supported_lang(Language) of
        true  -> z_convert:to_binary(Language);
        false -> <<"standard">>
    end.

%% Get property in the form title_en, body_nl etc.
get_language_property(Prop, Lang) ->
    z_convert:to_binary(
        atom_to_list(Prop) ++ "_" ++ atom_to_list(Lang)
    ).

%% Does Elasticsearch ship an analyzer for the language?
-spec is_supported_lang(string()) -> boolean().
is_supported_lang(Language) ->
    lists:member(Language, [
        "arabic", "armenian", "basque", "brazilian", "bulgarian", "catalan",
        "chinese", "czech", "danish", "dutch", "english", "finnish", "french",
        "galician", "german", "greek", "hindi", "hungarian", "indonesian",
        "irish", "italian", "japanese", "korean", "kurdish", "norwegian",
        "persian", "portuguese", "romanian", "russian", "spanish", "swedish",
        "turkish", "thai"
    ]).

%% @doc Get default translation for a property.
%%      Default is the (global) site language, not the Context's language.
default_translation(Trans, Context) ->
    z_trans:lookup_fallback(Trans, z_trans:default_language(Context), Context).

%% @doc Map date to ISO 8601 representation.
%%      Can be removed when https://github.com/zotonic/z_stdlib/pull/30 is merged
date_to_iso8601({{Y, _, _}, _} = DateTime) ->
    case z_datetime:undefined_if_invalid_date(DateTime) of
        undefined ->
            undefined;
        DateTime ->
            case z_dateformat:format(DateTime, "-m-d\\TH:i:s\\Z", en) of
                undefined ->
                    %% Can be undefined even if undefined_if_invalid_date is not.
                    undefined;
                Formatted ->
                    <<(year_to_iso8601(Y))/binary, Formatted/binary>>
            end
    end.

year_to_iso8601(Y) when Y < 0 ->
    iolist_to_binary(io_lib:format("-~4..0B", [abs(Y)]));
year_to_iso8601(Y) ->
    iolist_to_binary(io_lib:format("~4..0B", [Y])).


%% @doc Properties that don't need to be added to the search index.
-spec ignored_properties() -> [atom()].
ignored_properties() ->
    [
        crop_center,
        feature_show_address, feature_show_geodata,
        geocode_qhash, location_lng, location_lat,
        hasusergroup,
        installed_by,
        menu,
        pivot_geocode_qhash, pivot_location_lat, pivot_location_lng,
        managed_props, location_zoom_level,
        seo_noindex,
        tz,
        visible_for,
        %% possible custom props that will collide and we must therefore exclude:
        geolocation
    ].
