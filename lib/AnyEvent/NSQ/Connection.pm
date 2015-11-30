package AnyEvent::NSQ::Connection;

# ABSTRACT: NSQd TCP connection
# VERSION
# AUTHORITY

use strict;
use warnings;
use AnyEvent::Handle;
use Carp;
use JSON::XS ();
use Sys::Hostname;

# Options from the NSQ protocol spec, we might want to support later:
#   output_buffer_size=16384
#   output_buffer_timeout=250
#   tls_v1=False
#   tls_options=None
#   snappy=False
#   deflate=False
#   deflate_level=6
#   sample_rate=0
#   msg_timeout=30000
#   auth_secret=None

## Constructor

sub new {
  my ($class, %args) = @_;

  my $self = bless(
    { hostname        => hostname(),
      connect_timeout => undef,                 ## use kernel default
      requeue_delay   => 90,
      message_cb      => sub { print pop },
      error_cb        => sub { AE::log fatal => shift },
      disconnect_cb   => sub {  },
    },
    $class
  );

  $self->{host} = delete $args{host} or croak q{FATAL: required 'host' parameter is missing};
  $self->{port} = delete $args{port} or croak q{FATAL: required 'port' parameter is missing};

  for my $p (qw( client_id hostname heartbeat_interval user_agent connect_timeout connect_cb message_cb disconnect_cb error_cb heartbeat_cb )) {
    next unless exists $args{$p} and defined $args{$p};
    $self->{$p} = delete $args{$p};
    croak(qq{FATAL: parameter '$p' must be a CodeRef}) if $p =~ m{_cb$} and ref($self->{$p}) ne 'CODE';
  }

  croak(q{FATAL: required 'connect_cb' parameter is missing}) unless $self->{connect_cb};

  $self->connect;

  return $self;
}


## Connection control

sub connect {
  my ($self) = @_;
  return if $self->{handle};

  $self->{handle} = AnyEvent::Handle->new(
    connect => [$self->{host}, $self->{port}],

    on_prepare => sub { $self->{connect_timeout} },
    on_connect => sub { $self->_connected(@_) },

    on_connect_error => sub {
      $self->_log_error('(connect failed) ' . ($_[1] || $!));
      $self->_disconnected;
    },
    on_error => sub {
      $self->_log_error('(read error) ' . ($_[2] || $!));
      $self->_disconnected;
    },
    on_eof => sub {
      $self->_disconnected;
    },
  );

  return;
}

sub disconnect {
  my ($self, $cb) = @_;
  return unless my $hdl = $self->{handle};

  $hdl->push_write("CLS\012") if $self->{is_subscriber};

  $self->_on_next_success_frame(
    sub {
      $cb->($self, @_) if $cb;
      $self->_force_disconnect;
    }
  );

  return;
}

## Protocol API

sub identify {
  my ($self, @rest) = @_;
  return unless my $hdl = $self->{handle};

  my $cb = pop @rest;

  my $data = JSON::XS::encode_json($self->_build_identity_payload(@rest));
  $hdl->push_write("IDENTIFY\012" . pack('N', length($data)) . $data);

  $self->_on_next_success_frame(sub { $cb->($self, $self->{identify_info} = $_[1]) });

  return;
}

sub subscribe {
  my ($self, $topic, $chan, $cb) = @_;
  return unless my $hdl = $self->{handle};

  $cb //= sub { };
  $self->{is_subscriber} = 1;

  $hdl->push_write("SUB $topic $chan\012");
  $self->_on_next_success_frame($cb);

  return;
}

sub publish {
  my ($self, $topic, @data) = @_;
  return unless my $hdl = $self->{handle};

  my $cb;
  $cb = pop @data if ref($data[-1]) eq 'CODE' or !defined($data[-1]);
  return unless @data;

  my $body = join('', map { pack('N', length($_)) . $_ } @data);

  if (@data == 1) {
    $hdl->push_write("PUB $topic\012$body");
    $self->_on_next_success_frame($cb);
  }
  else {
    $hdl->push_write("MPUB $topic\012" . pack('N', length($body)) . pack('N', scalar(@data)) . $body);
    $self->_on_next_success_frame($cb);
  }

  return;
}

sub ready {
  my ($self, $n) = @_;
  return unless my $hdl = $self->{handle};

  $self->{ready_count} = $n;
  $self->{in_flight}   = 0;
  $hdl->push_write("RDY $n\012");

  return;
}

sub mark_as_done_msg {
  my ($self, $msg) = @_;
  return unless my $hdl = $self->{handle};

  my $id = ref($msg) ? $msg->{message_id} : $msg;

  $hdl->push_write("FIN $id\012");

  return;
}

sub requeue_msg {
  my ($self, $msg, $delay) = @_;
  return unless my $hdl = $self->{handle};

  my $id = ref($msg) ? $msg->{message_id} : $msg;
  my $attempts = ref($msg) ? $msg->{attempts} : 1;

  $delay = 0 unless defined $delay;
  $delay = $attempts * $self->{requeue_delay} if $delay < 0;

  $hdl->push_write("REQ $id $delay\012");

  return;
}

sub touch_msg {
  my ($self, $msg) = @_;
  return unless my $hdl = $self->{handle};

  my $id = ref($msg) ? $msg->{message_id} : $msg;
  $hdl->push_write("TOUCH $id\012");

  return;
}

sub nop {
  my ($self, $n) = @_;
  return unless my $hdl = $self->{handle};

  $hdl->push_write("NOP\012");

  return;
}


## Protocol helpers

sub _build_identity_payload {
  my ($self, @rest) = @_;

  my $ua = "AnyEvent::NSQ::Connection/" . ($AnyEvent::NSQ::Connection::VERSION || 'developer');

  my %data = (
    client_id => $self->{client_id},
    hostname  => $self->{hostname},
    heartbeat_interval => $self->{heartbeat_interval} // 30000,
    user_agent => $self->{user_agent} // $ua,
    ## TODO: output_buffer_size => ...,
    ## TODO: output_buffer_timeout => ...,
    ## TODO: sample_rate => ...,
    ## TODO: msg_timeout => ...,
    @rest,
    feature_negotiation => \1,
  );

  if (substr($data{user_agent}, -1) eq ' ') { $data{user_agent} .= $ua }

  for my $k (keys %data) {
    delete $data{$k} unless defined $data{$k};
  }

  return \%data;
}


## Connection setup and cleanup

## After a sucessfull connection, do all tasks expected of the protocol and our users
sub _connected {
  my ($self) = @_;

  $self->{connected} = 1;

  $self->_send_magic_identifier;
  $self->_start_recv_frames;

  $self->{connect_cb}->($self);
}

## Cleanup $self after a disconnect
sub _disconnected {
  my ($self) = @_;

  $self->{handle}->destroy;
  delete $self->{$_} for qw(handle connected);
  $_[0]->{disconnect_cb}->(@_) if $_[0]->{disconnect_cb};
}

## Try to be as clean as possible but force a disconnect
sub _force_disconnect {
  my ($self) = @_;
  return unless my $hdl = $self->{handle};

  $hdl->push_shutdown;
  $hdl->on_read(sub { });
  $hdl->on_eof(undef);
  $hdl->on_error(
    sub {
      delete $hdl->{rbuf};
      $self->_disconnected;
    }
  );
}


## low-level protocol management

sub _send_magic_identifier { $_[0]{handle}->push_write('  V2') }

sub _start_recv_frames {
  my ($self) = @_;
  my $hdl = $self->{handle};

  my @push_read_setup;
  @push_read_setup = (
    chunk => 8,
    sub {
      my ($size, $frame_type) = unpack('NN', $_[1]);
      $hdl->unshift_read(
        chunk => $size - 4,    ## remove size of frame_type...
        sub {
          my ($msg) = $_[1];

          my $action = $self->_process_incoming_frame($frame_type, $msg);
          return $hdl->push_read(@push_read_setup) if $action;

          $self->_force_disconnect;
        }
      );
    }
  );

  ## Start with first frame...
  $hdl->push_read(@push_read_setup);
}

## Decide which type of frame we have, and take care of it
sub _process_incoming_frame {
  my ($self, $frame_type, $msg) = @_;

  my $res;
  if    ($frame_type == 0) { $res = $self->_process_success_frame($msg) }
  elsif ($frame_type == 1) { $res = $self->_process_error_frame($msg) }
  elsif ($frame_type == 2) { $res = $self->_process_message_frame($msg) }
  else                     { AE::log alert => "Unknown message frame ($frame_type)";
                             $self->_force_disconnect }

  return $res;
}

## Proces success frames, both plain OK, JSON-encoded and _heartbeat_ success messages
sub _process_success_frame {
  my ($self, $msg) = @_;

  if ($msg eq '_heartbeat_') {
    $self->nop;
    $self->{heartbeat_cb}->($self) if 'CODE' eq ref $self->{heartbeat_cb};
  }
  else {
    my $info = { msg => $msg };
    if ($msg =~ m/^\s*[{]/) {
      $info = eval { JSON::XS::decode_json($msg) };
      unless ($info) {
        $self->_log_error(qq{unexpected/invalid JSON response '$msg'});
        $self->_disconnected;
        $self->{error_cb}->($msg) if ref $self->{error_cb};
      }
    }

    my $cb = shift @{ $self->{success_cb_queue} || [] };
    $cb->($self, $info) if $cb;
  }

  return 'keep_reading_frames';
}

## Manage queue of pending success frame handlers
sub _on_next_success_frame { push @{ $_[0]->{success_cb_queue} }, $_[1] }


## Processing of error messages, just signal and
sub _process_error_frame {
  my ($self, $msg) = @_;

  $self->_log_error(qq{received error frame '$msg'});
  $self->_disconnected;
  $self->{error_cb}->($msg) if ref $self->{error_cb};
}

## Process regular message frames, callback, and deal with RDY state
sub _process_message_frame {
  my ($self, $msg) = @_;

  my ($t1, $t2, $attempts, $message_id) = unpack('NNnA16', substr($msg, 0, 26, ''));
  $msg = {
    attempts   => $attempts,
    message_id => $message_id,
    tstamp     => ($t2 | ($t1 << 32)),
    message    => $msg,
  };

  $self->{in_flight}++;
  $self->{message_cb}->($self, $msg) if $self->{message_cb};

  ## FIXME: this logic was more of infered than learned, but I remember seeing 25% somewhere
  ## FIXME: move RDY state processing to Reader
  $self->ready($self->{ready_count})
    if $self->{ready_count} and $self->{in_flight} / $self->{ready_count} > .25;

  return 'keep_reading_frames';
}


## Error logging
sub _log_error {
  my ($self, $err) = @_;

  $self->{error_cb}->($err, qq{FATAL: dropping connection to host '$self->{host}' port $self->{port}, reason: $err});

  return;
}

1;
