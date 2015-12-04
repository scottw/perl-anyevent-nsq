#!perl

use strict;
use warnings;
use Test::More;
use AnyEvent;
use AnyEvent::Socket ();
use AnyEvent::NSQ::Connection;
use Data::Dumper;

plan $ENV{NSQD_HOSTPORT}
  ? (tests => 7)
  : (skip_all => "NSQD_HOSTPORT environment variable not set");

my $cv_connect    = AE::cv;
my $cv_heartbeat  = AE::cv;
my $cv_error      = AE::cv;

my @nsqd = split /,/ => $ENV{NSQD_HOSTPORT};

my ($host, $port) = AnyEvent::Socket::parse_hostport($nsqd[0]);

my $c = AnyEvent::NSQ::Connection->new(
    host               => $host,
    port               => $port,
    user_agent         => 'some-nsq-agent',
    connect_cb         => sub { $cv_connect->send(1) },
    heartbeat_interval => 1000,
    heartbeat_cb       => sub { $cv_heartbeat->send(1) },
    error_cb           => sub { $cv_error->send(shift) },
);

ok($cv_connect->recv, "AE::NSQ connection created");

my $cv_identify = AE::cv;
$c->identify(sub { $cv_identify->send(1) });
ok($cv_identify->recv, "identify response");

ok($cv_heartbeat->recv, "AE::NSQ heartbeat received");

delete $c->{heartbeat_cb};

my $cv_subscribe = AE::cv;
$c->subscribe(topeka => news => sub { $cv_subscribe->send(@_) });
ok($cv_subscribe->recv, "subscribed");

my $cv_pub = AE::cv;
$c->publish(topeka => "hi mom!", sub { $cv_pub->send("publish") });
is($cv_pub->recv, "publish", "published");

my $cv_message = AE::cv;
$c->{message_cb} = sub { shift; $cv_message->send(shift) };
$c->ready(1);
my $msg = $cv_message->recv;
is($msg->{message}, "hi mom!", "message received");
$c->ready(0);

$c->mark_as_done_msg($msg);

$c->subscribe(topeka => weather => sub { });

like($cv_error->recv, qr/E_INVALID cannot SUB in current state/,
     "cannot resubscribe");

exit;
