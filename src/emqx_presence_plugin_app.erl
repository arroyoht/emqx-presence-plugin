-module(emqx_presence_plugin_app).

-behaviour(application).

-emqx_plugin(?MODULE).

-export([ start/2
        , stop/1
        ]).

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqx_presence_plugin_sup:start_link(),
    emqx_presence_plugin:load(application:get_all_env()),
    {ok, Sup}.

stop(_State) ->
    emqx_presence_plugin:unload().

