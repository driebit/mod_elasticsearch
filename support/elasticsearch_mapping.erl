%% Elasticsearch mapping
-module(elasticsearch_mapping).
-author("Driebit <tech@driebit.nl>").

-include("zotonic.hrl").

-export([
    map_rsc/2,
    default_mapping/2,
    hash/1
]).

%% Map a Zotonic resource
map_rsc(Id, Context) ->
    Props = m_rsc:get(Id, Context),
    [
        {category, m_rsc:is_a(Id, Context)},
        {pivot_title, default_translation(proplists:get_value(title, Props), Context)}
    ] ++
    map_location(Id, Context) ++
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
        Props
    ) ++
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

map_property({Key, Value}) ->
    % Exclude some properties that don't need to be added to index
    IgnoredProps = [
        crop_center,
        feature_show_address, feature_show_geodata,
        geocode_qhash, location_lng, location_lat,
        hasusergroup,
        installed_by,
        pivot_geocode_qhash, pivot_location_lat, pivot_location_lng,
        managed_props, blocks, location_zoom_level,
        seo_noindex,
        tz,
        visible_for,
        %% possible custom props that will collide and we must therefore exclude:
        geolocation
    ],
    case lists:member(Key, IgnoredProps) of
        true ->
            undefined;
        false ->
            case Value of
                {trans, Translations} ->
                    lists:map(
                        fun({LangCode, Translation}) ->
                            %% Change keys to contain language code: title_en etc.
                            LangKey = get_language_property(Key, LangCode),
                            {LangKey, z_html:strip(Translation)}
                        end,
                        Translations
                    );
                _ ->
                    {Key, map_value(Value)}
            end
    end.

% Make Zotonic resource value Elasticsearch-friendly
map_value({{Y, M, D}, {H, I, S}} = DateTime) when
    is_integer(Y), is_integer(M), is_integer(D),
    is_integer(H), is_integer(I), is_integer(S) ->
    case z_convert:to_isotime(DateTime) of
        [] ->
            %% e.g., ?ST_JUTTEMIS or invalid dates
            null;
        <<"">> ->
            null;
        Value ->
            z_convert:to_binary(Value)
    end;
map_value({{_, _, _}, {_, _, _}}) ->
    %% Invalid date
    null;
map_value({trans, Translations}) ->
    lists:map(
        % Strip HTML tags from translated props. This is only needed for body,
        % but won't hurt the other translated props.
        %
        fun({LangCode, Translation}) ->
            {LangCode, z_html:strip(Translation)}
        end,
        Translations
    );
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
            }
        },
        <<"dynamic_templates">> =>
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
            )
    },
    {hash(Mapping), Mapping}.

%% @doc Generate unique SHA1-based hash for a mapping
-spec hash(map()) -> binary().
hash(Map) ->
    z_string:to_lower(z_utils:hex_encode(crypto:hash(sha, jsx:encode(Map)))).

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
%%      Start from the site language, fall back to the first non-empty
%%      translation.
default_translation({trans, Translations}, Context) ->
    %% First try site language
    Language = z_convert:to_atom(m_config:get_value(i18n, language, Context)),
    case proplists:get_value(Language, Translations) of
        <<>> ->
            %% Look up first non-empty translation
            case lists:filter(fun({_Language, Value}) -> Value =/= <<>> end, Translations) of
                [Hd|_] ->
                    Hd;
                [] ->
                    undefined
            end;
        Translated ->
            Translated
    end;
default_translation(Prop, _Context) ->
    Prop.
