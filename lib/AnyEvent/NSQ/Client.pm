package AnyEvent::NSQ::Client;

# ABSTRACT: base class for NSQ.io consumers and producers
# VERSION
# AUTHORITY

use strict;
use warnings;
use AnyEvent;
use AnyEvent::Socket ();
use AnyEvent::HTTP 'http_get';
use JSON::XS 'decode_json';
use Carp 'croak';
use AnyEvent::NSQ::Connection;
use feature 'current_sub';

#### Public API

## constructor
sub new {
  my ($class, %args) = @_;
  my $self = bless {}, $class;

  $self->_parse_args(\%args);
  $self->_connect();

  return $self;
}

## disconnect from our pool of nsqd connection
sub disconnect {
  my ($self, $cb) = @_;

  $_->{conn}->disconnect($cb) for values %{ $self->{nsqd_conns} };
}

## Publish a single or multiple message - callback is only called if we succedd
sub publish {
  my ($self, $topic, @data) = @_;

  my $conn = $self->_random_connected_conn;
  croak "ERROR: there no active connections at this moment," unless $conn;

  my @args;
  if (ref($data[-1]) eq 'CODE' or !defined($data[-1])) {
    my $cb = pop @data;

    if ($cb) {
      my @cb_data = @data;
      push @data, sub { $cb->($self, $topic, \@cb_data, @_) }
    }
  }

  return $conn->publish($topic, @data);
}

sub ready {
  my ($self, $ready_count) = @_;

  $_->{conn}->ready($ready_count) for values %{ $self->{nsqd_conns} };

  return;
}

#### Argument parsing

## Parse all common arguments
sub _parse_args {
  my ($self, $args) = @_;

  $self->{disconnect_cb} = delete($args->{disconnect_cb}) || sub { };
  $self->{error_cb}      = delete($args->{error_cb})      || sub { croak($_[1]) };

  $self->{connect_cb}  = delete($args->{connect_cb})  || sub { };
  $self->{identify_cb} = delete($args->{identify_cb}) || sub { };

  $self->{lookup_cb} = delete($args->{lookup_cb}) || sub { };

  $self->{lookupd_poll_interval} = delete($args->{lookupd_poll_interval}) || 30;

  for my $arg (qw( client_id hostname connect_timeout topic )) {
    $self->{$arg} = delete($args->{$arg}) if exists $args->{$arg};
  }

  if (my $lookupd_http_addresses = delete $args->{lookupd_http_addresses}) {
    $lookupd_http_addresses = [$lookupd_http_addresses] unless ref($lookupd_http_addresses) eq 'ARRAY';
    $self->{lookupd_http_addresses} = $lookupd_http_addresses;
    $self->{use_lookupd}            = 1;
  }

  if (my $nsqd_tcp_addresses = delete $args->{nsqd_tcp_addresses}) {
    croak(q{FATAL: only one of 'lookupd_http_addresses' and 'nsqd_tcp_addresses' is allowed}) if $self->{use_lookupd};

    $nsqd_tcp_addresses = [$nsqd_tcp_addresses] unless ref($nsqd_tcp_addresses) eq 'ARRAY';
    $self->{nsqd_tcp_addresses} = $nsqd_tcp_addresses;
    $self->{use_lookupd}        = 0;
  }

  ## There can be only one, there must be at least one
  croak(q{FATAL: one of 'nsqd_tcp_addresses' or 'lookup'}) unless defined $self->{use_lookupd};
}


#### Connection management

## support both modes of operation, direct or with lookupd discovery
sub _connect {
  my $self = shift;

  if ($self->{use_lookupd}) {
    $self->_start_lookupd_pollers;
  }
  else {
    $self->_start_nsqd_connections;
  }

  return;
}

## direct nsqd connection
sub _start_nsqd_connections {
  my ($self) = @_;

  for my $nsqd_tcp_address (@{ $self->{nsqd_tcp_addresses} }) {
    $self->_start_nsqd_connection($nsqd_tcp_address, reconnect => 1);
  }
}

## nsqlookupd support - not there yet
sub _start_lookupd_pollers {
  my $self = shift;

  ## polling event: "it's time to poll the lookupds!"
  $self->{_evt_lookupd_poll} = AE::cv;
  $self->{_evt_lookupd_poll}->cb(sub {
      my $cv = AE::cv;
      $cv->begin(sub { $self->_start_nsqd_connections;
                       $self->{lookup_cb}->() if ref $self->{lookup_cb} eq 'CODE' });

      ## reset the list of nsqd for this topic
      $self->{nsqd_tcp_addresses} = [];

      ## FIXME: check for existing scheme, sanitize/encode $self->{topic}, etc.
      ## FIXME: set timeout to a low value (lower than the timer interval at most)
      for my $addr ( map { 'http://' . $_ . '/lookup?topic=' . $self->{topic} } @{$self->{lookupd_http_addresses}}) {
          $cv->begin;

          AE::log debug => "Querying lookupd ($addr) for topic " . $self->{topic};
          http_get $addr,
            headers => { "User-Agent" => "AE::NSQ::Client",
                         Accept       => "application/json" },
                           sub { my ($json, $headers) = @_;
                                 my $data = eval { decode_json $json } // {};

                                 AE::log debug =>
                                     sub { require Data::Dumper;
                                           "Lookupd ($addr) response: " . Data::Dumper::Dumper($data); };
                                 push @{$self->{nsqd_tcp_addresses}}, map { $_->{hostname} . ':' . $_->{tcp_port} } @{$data->{data}->{producers}}
                                   if ref $data->{data};

                                 $cv->end;
                             };
      }

      $cv->end;

      $self->{_evt_lookupd_poll} = AE::cv;
      $self->{_evt_lookupd_poll}->cb(__SUB__);
  });

  ## start the timer which invokes the polling event
  $self->{_lookupd_poller} =
    AnyEvent->timer (after => 0, interval => $self->{lookupd_poll_interval}, cb => sub { $self->{_evt_lookupd_poll}->() });
}


#### nsqd pool connection management

## connect to a single element of the pool
sub _start_nsqd_connection {
  my ($self, $nsqd_tcp_address, %args) = @_;

  AE::log trace => "Request for TCP connection to $nsqd_tcp_address";
  my $conns = $self->{nsqd_conns} ||= {};
  if ($conns->{$nsqd_tcp_address}) {
      AE::log debug => "Already have TCP connection to $nsqd_tcp_address";
      return;
  }

  my ($host, $port) = AnyEvent::Socket::parse_hostport($nsqd_tcp_address);  ## must be ip/hostname:port
  croak(qq{FATAL: could not parse '$nsqd_tcp_address' as a valid address/port combination}) unless $host and $port;

  my %conn = (host => $host, port => $port);
  for my $arg (qw( client_id hostname error_cb connect_timeout )) {
    $conn{$arg} = $self->{$arg} if exists $self->{$arg};
  }

  $conn{connect_cb}    = sub { $self->_connected(@_, $nsqd_tcp_address) };
  $conn{disconnect_cb} = sub { $self->_disconnected(@_) };

  AE::log trace => sub { require Data::Dumper;
                         "Now instantiating AE::NSQ::Connection with " . Data::Dumper::Dumper(\%conn); };
  $conns->{$nsqd_tcp_address}{conn}  = AnyEvent::NSQ::Connection->new(%conn);
  $conns->{$nsqd_tcp_address}{state} = 'connecting';

  return;
}

## return one connection that is connected
sub _random_connected_conn {
  ## FIXME: yeah, Sony-style random going on :)
  return (values %{ $_[0]{nsqd_conns} })[0]->{conn};
}


#### Hooks for the main states of the connection
sub _connected {
    my $self             = shift;
    my $nsqd_tcp_address = pop;

    $self->{connect_cb}->(@_) if $self->{connect_cb};
    $_[0]->identify(
        sub {
            $self->_identified(@_);
            $self->{nsqd_conns}->{$nsqd_tcp_address}->{state} = 'connected'
        }
    );
}

sub _identified   { $_[0]->{identify_cb}->(@_)   if $_[0]->{identify_cb} }
sub _disconnected { $_[0]->{disconnect_cb}->(@_) if $_[0]->{disconnect_cb} }

1;

__END__
'data' => {
            'producers' => [
                            {
                               'version' => '0.3.6',
                               'http_port' => 4151,
                               'broadcast_address' => 'lookupd',
                               'tcp_port' => 4150,
                               'remote_address' => '172.18.0.3:52231',
                               'hostname' => 'd6a8a0d49959'
                             }
                           ],
            'channels' => []
          }
