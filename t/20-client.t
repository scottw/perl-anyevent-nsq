#!perl
use strict;
use warnings;
use Test::More;
use feature 'say';
use AnyEvent;
use AnyEvent::Socket ();
use AnyEvent::NSQ::Connection;
use AnyEvent::NSQ::Client;
use Data::Dumper;

plan $ENV{NSQD_HOSTPORT} && $ENV{NSQLOOKUPD_HOSTPORT}
  ? (tests => 10)
  : (skip_all => "NSQD_HOSTPORT and NSQLOOKUPD_HOSTPORT environment variables not set");

my @nsqd = split /,/ => $ENV{NSQD_HOSTPORT};

my $hn = `hostname`;
chomp $hn;

my $cv_lookup = AE::cv;

my $topic = 'AE-NSQ-Client-' . time . '-' . $$;

## no hosts should have this topic
my $look = AnyEvent::NSQ::Client->new(
    lookupd_poll_interval  => 2,
    lookup_cb              => sub { $cv_lookup->send },  ## has to be a closure
    client_id              => 'lookupd-client',
    hostname               => $hn,
    topic                  => $topic,
    lookupd_http_addresses => [$ENV{NSQLOOKUPD_HOSTPORT}],
);

$cv_lookup->recv;
$cv_lookup = AE::cv;  ## reset the condvar

is_deeply($look->{nsqd_tcp_addresses}, [], "no nsqd found");

## now publish and create a topic
{
    my ($pub_host, $pub_port) = AnyEvent::Socket::parse_hostport($nsqd[0]);

    my $cv_step = AE::cv;
    my $pubb = AnyEvent::NSQ::Connection->new(
        host       => $pub_host,
        port       => $pub_port,
        connect_cb => sub { $cv_step->send },
    );
    $cv_step->recv;

    $cv_step = AE::cv;
    $pubb->publish($topic => "tacos are good", sub { $cv_step->send });
    $cv_step->recv;
}

$cv_lookup->recv;
$cv_lookup = AE::cv;  ## reset the condvar

is(scalar(@{$look->{nsqd_tcp_addresses}}), 1, "found a publisher");

## publish to another nsqd on the same topic
{
    my ($pub_host, $pub_port) = AnyEvent::Socket::parse_hostport($nsqd[1]);

    my $cv_step = AE::cv;
    my $pubb = AnyEvent::NSQ::Connection->new(
        host       => $pub_host,
        port       => $pub_port,
        connect_cb => sub { $cv_step->send },
    );
    $cv_step->recv;

    $cv_step = AE::cv;
    $pubb->publish($topic => "tacos are badd", sub { $cv_step->send });
    $cv_step->recv;
}

$cv_lookup->recv;

is(scalar(@{$look->{nsqd_tcp_addresses}}), 2, "found a publisher");

exit;
