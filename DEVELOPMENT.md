# AnyEvent::NSQ Development

This is a work-in-progress! Not everything works yet.

Our goal is to make AnyEvent::NSQ a full-featured, top-tier NSQ
client. To do that, we have some work to do:

[] lookupd discovery (scottw: wip)
[] connection backoff
[] max-in-flight/RDY balancing
[] starvation handling
[] snappy support
[] tls support
[] deflate support

See [Building Client
Libraries](http://nsq.io/clients/building_client_libraries.html) and
[TCP Protocl Spec](http://nsq.io/clients/tcp_protocol_spec.html) for
details.

We're also working on an RPC adapter (something like
[this](https://github.com/project-fifo/ensq_rpc)) which will present a
blocking interface:

    my $resp = $c->request("topic", "message");

and under the hood will create a new subscription to an ephemeral
topic and pass that channel name via a well-known message format. It
will then publish the message. When whatever service handles the
message, it checks for the response topic and publishes its reply to
it. Once the blocking request has its answer over the ephemeral topic,
it unsubscribes and returns the response to the caller.

## Testing

We use TDD here, but not everything is passing yet (I break things all
the time). Everything *should* pass, however, and I'll try to tag
commits where that is the case.

We don't have a mocked interface yet, so to test this you'll want to
run `nsqd` and `nsqlookupd`. Docker is really easy for this, but nsq
provides binaries for some dev environments.

To run the `t/20-client.t` tests, you'll need 2 nsqds and 1 lookupd
running. I'm using Docker, so first I create a network:

    $ docker network create --driver bridge nsq-plane

Then I start a lookupd:

    $ docker run --name lookupd --net nsq-plane -p 4160:4160 -p 4161:4161 nsqio/nsq /nsqlookupd

Then I can `docker-compose up` to get the nsqds running. Finally I can run my test:

    $ PERL_ANYEVENT_VERBOSE=5 NSQD_HOSTPORT=$(docker-machine ip nsq-dev):4150,$(docker-machine ip nsq-dev):4154 NSQLOOKUPD_HOSTPORT=$(docker-machine ip nsq-dev):4161 cm prove -lv t/20-client.t

