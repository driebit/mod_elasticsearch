%% Elasticsearch mapping
-module(elasticsearch_mapping).
-author("Driebit <tech@driebit.nl>").

-include("zotonic.hrl").

-export([
    map_rsc/2,
    default_mapping/2
]).

%% Map a Zotonic resource
map_rsc(Id, Context) ->
    [
        {category, m_rsc:is_a(Id, Context)}
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
        m_rsc:get(Id, Context)
    ).

map_location(Id, Context) ->
    case {m_rsc:p(Id, location_lat, Context), m_rsc:p(Id, location_lng, Context)} of
        {_, undefined} ->
            [];
        {undefined, _} ->
            [];
        {Lat, Lng} ->
            [{location, [
                {lat, z_convert:to_float(Lat)},
                {lon, z_convert:to_float(Lng)}
            ]}]
    end.

map_property({Key, Value}) ->
    % Exclude some properties that don't need to be added to index
    IgnoredProps = [
        geocode_qhash, location_lng, location_lat,
        pivot_geocode_qhash, pivot_location_lat, pivot_location_lng,
        managed_props, blocks, location_zoom_level
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

%% Get a default Elasticsearch mapping for Zotonic resources
default_mapping(Type, Context) ->
    [{Type, [
        {properties, [
            % location_lat and location_lon are stored in a location property
            {location, [
                {type, <<"geo_point">>}
            ]},
            {blocks, [
                {type, <<"nested">>}
            ]}
        ]
            ++
            % If mod_translation is enabled, set language-specific analyzers
            case z_module_manager:module_exists(mod_translation) of
                true ->
                    % Translations enabled, so add translated properties
                    lists:foldl(
                        fun(Prop, Acc) ->
                            % For each enabled language, construct an analyzer
                            Langs = lists:map(
                                fun({LangCode, _}) ->
                                    {get_language_property(Prop, LangCode), [
                                        {type, string},
                                        {analyzer, get_analyzer(LangCode)}
                                    ]}
                                end,
                                m_translation:language_list_enabled(Context)
                            ),
                            Acc ++ Langs
                        end,
                        [],
                        [title, short_title, body, summary]
                    );

                false ->
                    %% Translations not enabled, so add single untranslated property
                    []
            end
        }
    ]}].

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
