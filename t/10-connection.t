#!perl

use strict;
use warnings;
use Test::More;
use AnyEvent;
use AnyEvent::NSQ::Connection;
use Data::Dumper;

if ($ENV{NSQD_HOST}) {
    plan tests => 6;
}

else {
    plan skip_all => "NSQD_HOST environment variable not set";
}

my $cv_connect   = AE::cv;
my $cv_heartbeat = AE::cv;

my $c = AnyEvent::NSQ::Connection->new(
    host               => $ENV{NSQD_HOST},
    port               => $ENV{NSQD_PORT} // 4150,
    user_agent         => 'some-nsq-agent',
    connect_cb         => sub { $cv_connect->send(1) },
    heartbeat_interval => 1000,
    heartbeat_cb       => sub { $cv_heartbeat->send(1) },
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

$c->mark_as_done_msg($msg->{message_id});


exit;
