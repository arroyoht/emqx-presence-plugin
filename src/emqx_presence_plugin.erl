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
-export([ on_message_delivered/3
        , on_message_dropped/4 ]).

%% Called when the plugin application start
load(Env) ->
    emqx:hook('session.subscribed',  {?MODULE, on_session_subscribed, [Env]}),
    emqx:hook('session.unsubscribed',{?MODULE, on_session_unsubscribed, [Env]}),
    emqx:hook('session.terminated',  {?MODULE, on_session_terminated, [Env]}),
    emqx:hook('message.delivered',   {?MODULE, on_message_delivered, [Env]}),
    emqx:hook('message.dropped',     {?MODULE, on_message_dropped, [Env]}).

%%--------------------------------------------------------------------
%% Session Lifecycle Hooks
%%--------------------------------------------------------------------

on_session_subscribed(#{clientid := ClientId}, Topic, _SubOpts, _Env) ->
    publish_presence(online, ClientId, Topic).

on_session_unsubscribed(#{clientid := ClientId}, Topic, _Opts, _Env) ->
    publish_presence(offline, ClientId, Topic).

on_session_terminated(#{clientid := ClientId}, _Reason, SessInfo, _Env) ->
    Topics = maps:keys(maps:get(subscriptions, SessInfo)),
    publish_presences(Topics, ClientId).

%%--------------------------------------------------------------------
%% Message PubSub Hooks
%% Event delivery generates online/offline messages on success/failure
%%--------------------------------------------------------------------

% message delivered generates an online event
on_message_delivered(#{clientid := ClientId}, #message{topic = Topic}, _Env) ->
    publish_presence(online, ClientId, Topic).

% message dropped should generate a special event that disconnects all devices from a merchant
on_message_dropped(#message{topic = Topic}, _By, no_subscribers, _Env) ->
    case publish_presence(offline, <<"$ALL">>, Topic) of
        published -> 
            io:format("Message to merchant topic ~s dropped, no client is listening~n", [Topic]);
        _ -> ok
    end;
on_message_dropped(_Message, _By, _Reason, _Env) ->
    ok.

%% Called when the plugin application stop
unload() ->
    emqx:unhook('session.subscribed',  {?MODULE, on_session_subscribed}),
    emqx:unhook('session.unsubscribed',{?MODULE, on_session_unsubscribed}),
    emqx:unhook('session.terminated',  {?MODULE, on_session_terminated}),
    emqx:unhook('message.delivered',   {?MODULE, on_message_delivered}),
    emqx:unhook('message.dropped',     {?MODULE, on_message_dropped}).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

% Publish offline presence from a list of topics
publish_presences([], _) -> ok;
publish_presences([Topic | Topics], ClientId) ->
    publish_presence(offline, ClientId, Topic),
    publish_presences(Topics, ClientId).

% Publish presence event depending on status
publish_presence(online, ClientId, <<"events/orders/", Merchant/binary>>) ->
    io:format("Device ~s is online with merchant ~s~n", [ClientId, Merchant]),
    Presence = connected_presence([Merchant], <<"ONLINE">>),
    publish_message(topic(ClientId), Presence);
publish_presence(offline, ClientId, <<"events/orders/", Merchant/binary>>)  ->
    io:format("Device ~s is offline with merchant ~s~n", [ClientId, Merchant]),
    Presence = connected_presence([Merchant], <<"OFFLINE">>),
    publish_message(topic(ClientId), Presence);
publish_presence(_, _, _) -> 
    ok.

% publish message payload to a topic
publish_message(Topic, Payload) ->
    case emqx_json:safe_encode(Payload) of
        {ok, Encoded} ->
            io:format("Publishing message ~s on topic ~s~n", [Encoded, Topic]),
            emqx_broker:safe_publish(
              make_msg(1, Topic, Encoded)),
            published;
        {error, _Reason} ->
            ok
    end.

% compose device status topic
topic(ClientId) ->
    iolist_to_binary(["devices/", ClientId, "/status"]).

% compose emqx message object
make_msg(QoS, Topic, Payload) ->
    emqx_message:make(
        ?MODULE, QoS, Topic, iolist_to_binary(Payload)).

% compose status payload
connected_presence(Merchants, Status) ->
      #{merchants => Merchants,
      status => Status}.