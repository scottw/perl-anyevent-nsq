#!perl

use strict;
use warnings;
use Test::More;
use feature 'say';
use AnyEvent;
use AnyEvent::NSQ::Client;
use Data::Dumper;

plan $ENV{NSQD_HOSTPORT} && $ENV{NSQLOOKUPD_HOSTPORT}
  ? (tests => 10)
  : (skip_all => "NSQD_HOSTPORT and NSQLOOKUPD_HOSTPORT environment variables not set");

my $cv_connect  = AE::cv;
my $cv_identify = AE::cv;

my $hn = `hostname`;
chomp $hn;
my $c = AnyEvent::NSQ::Client->new(
    connect_cb         => sub { $cv_connect->send(@_) },
    identify_cb        => sub { $cv_identify->send(@_) },
    client_id          => 'some-test-client',
    hostname           => $hn,
    nsqd_tcp_addresses => [ $ENV{NSQD_HOSTPORT} ],
);

ok($cv_connect->recv,  "connection created");
ok($cv_identify->recv, "identify sent");

