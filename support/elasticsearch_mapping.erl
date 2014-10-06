%% Elasticsearch mapping
-module(elasticsearch_mapping).
-author("Driebit <tech@driebit.nl>").

-include("zotonic.hrl").

-export([
    default_mapping/2
]).

%% Get a default Elasticsearch mapping for Zotonic resources
default_mapping(Type, Context) ->
    [{Type, [{
        properties, [
            % location_lat and location_lon are stored in a location property
            {location, [
                {type, <<"geo_point">>}
            ]}
        ]
            ++
            % If mod_translation is enabled, set language-specific analyzers
            case z_module_manager:module_exists(mod_translation) of
                true ->
                    lists:map(
                        fun(Prop) ->
                            Langs = lists:map(
                                fun({LangCode, _}) ->
                                    {LangCode, [
                                        {type, string},
                                        {analyzer, get_analyzer(LangCode)}
                                    ]}
                                end,
                                m_translation:language_list_enabled(Context)
                            ),
                            {Prop, [
                                {type, <<"nested">>},
                                {properties, Langs}
                            ]}
                        end,
                        [title, short_title, body, summary]
                    );
                false -> []
            end
        }]
    }].

%% Get analyzer for a language
get_analyzer(LangCode) ->
    Language =
        string:to_lower(
            iso639:lc2lang(z_convert:to_list(LangCode))
        ),

    case is_supported_lang(Language) of
        true  -> z_convert:to_binary(Language);
        false -> <<"standard">>
    end.

%% Does Elasticsearch ship an analyzer for the language?
is_supported_lang(Language) ->
    lists:member(Language, [
        "arabic", "armenian", "basque", "brazilian", "bulgarian", "catalan",
        "chinese", "czech", "danish", "dutch", "english", "finnish", "french",
        "galician", "german", "greek", "hindi", "hungarian", "indonesian",
        "irish", "italian", "japanese", "korean", "kurdish", "norwegian",
        "persian", "portuguese", "romanian", "russian", "spanish", "swedish",
        "turkish", "thai"
    ]).
