-module(emqx_presence_plugin).

-include_lib("emqx/include/emqx.hrl").

-export([ load/1
        , unload/0
        ]).

%% Session Lifecircle Hooks
-export([ on_session_subscribed/4
        , on_session_unsubscribed/4
        , on_session_terminated/4 ]).

%% Message Pubsub Hooks
-export([ on_message_acked/3 ]).

%% Called when the plugin application start
load(Env) ->
    emqx:hook('session.subscribed',  {?MODULE, on_session_subscribed, [Env]}),
    emqx:hook('session.unsubscribed',{?MODULE, on_session_unsubscribed, [Env]}),
    emqx:hook('session.terminated',  {?MODULE, on_session_terminated, [Env]}),
    emqx:hook('message.acked',       {?MODULE, on_message_acked, [Env]}).

%%--------------------------------------------------------------------
%% Session Lifecycle Hooks
%%--------------------------------------------------------------------

on_session_subscribed(#{clientid := ClientId}, Topic, _SubOpts, _Env) ->
    Parsed = match_and_parse(Topic),
    publish_presence(online, ClientId, Parsed).

on_session_unsubscribed(#{clientid := ClientId}, Topic, _Opts, _Env) ->
    Parsed = match_and_parse(Topic),
    publish_presence(offline, ClientId, Parsed).

on_session_terminated(_ClientInfo = #{clientid := ClientId}, _Reason, SessInfo, _Env) ->
    Topics = maps:keys(maps:get(subscriptions, SessInfo)),
    publish_presences(Topics, ClientId).

%%--------------------------------------------------------------------
%% Message PubSub Hooks
%%--------------------------------------------------------------------

%% Transform message and return

on_message_acked(_ClientInfo = #{clientid := ClientId}, Message, _Env) ->
    io:format("Message acked by client(~s): ~s~n",
              [ClientId, emqx_message:format(Message)]).

%% Called when the plugin application stop
unload() ->
    emqx:unhook('session.subscribed',  {?MODULE, on_session_subscribed}),
    emqx:unhook('session.unsubscribed',{?MODULE, on_session_unsubscribed}),
    emqx:unhook('session.terminated',  {?MODULE, on_session_terminated}),
    emqx:unhook('message.acked',       {?MODULE, on_message_acked}).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

% Match topic and return parsed merchantId if success
match_and_parse(Topic) ->
    case re:run(Topic, "^events/orders/+") of
        {match, _} -> lists:last(re:split(Topic, "/"));
        _ -> ignore
    end.

% Publish offline presence from a list of topics
publish_presences([], _) -> ok;
publish_presences([H|T], ClientId) ->
    Parsed = match_and_parse(H),
    publish_presence(offline, ClientId, Parsed),
    publish_presences(T, ClientId).

% Publish presence event depending on status
publish_presence(_, _, ignore) -> ok;
publish_presence(online, ClientId, Merchant) ->
    io:format("Device ~s is online with merchant ~s~n", [ClientId, Merchant]),
    Presence = connected_presence(ClientId, [Merchant], "ONLINE"),
    publish_message(topic(ClientId), Presence);
publish_presence(offline, ClientId, Merchant) ->
    io:format("Device ~s is offline with merchant ~s~n", [ClientId, Merchant]),
    Presence = connected_presence(ClientId, [Merchant], "OFFLINE"),
    publish_message(topic(ClientId), Presence).

% publish message payload to a topic
publish_message(Topic, Payload) ->
    case emqx_json:safe_encode(Payload) of
        {ok, Encoded} ->
            io:format("Publishing message ~s on topic ~s~n", [Encoded, Topic]),
            emqx_broker:safe_publish(
              make_msg(1, Topic, Encoded));
        {error, _Reason} ->
            ok
    end.

topic(ClientId) ->
    iolist_to_binary(["devices/", ClientId, "/status"]).

make_msg(QoS, Topic, Payload) ->
    emqx_message:make(
        ?MODULE, QoS, Topic, iolist_to_binary(Payload)).

connected_presence(ClientId, Merchants, Status) ->
    #{clientId => ClientId,
      merchants => Merchants,
      status => Status}.