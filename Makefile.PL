use 5.018000;
use strict;
use warnings;
use ExtUtils::MakeMaker;

my %PREREQ_PM = (
    'EV'        => '0.0',
    'AnyEvent'  => '0.0',
);

WriteMakefile(
    NAME                => 'AnyEvent::NSQ',
    AUTHOR              => 'Scott Wiersdorf <scottw@bluehost.com>',
    VERSION_FROM        => 'lib/AnyEvent/NSQ.pm',
    ABSTRACT_FROM       => 'lib/AnyEvent/NSQ.pm',
    ($ExtUtils::MakeMaker::VERSION >= 6.3002 ? (LICENSE => 'perl') : ()),
    PL_FILES            => {},
    CONFIGURE_REQUIRES  => {
        'ExtUtils::MakeMaker'   => '6.56',
    },
    BUILD_REQUIRES  => {
        'Test::More'            => '0.98',
    },
    MIN_PERL_VERSION    => '5.018000',
    PREREQ_PM           => \%PREREQ_PM,
    META_MERGE          => {
        resources   => {
            'meta-spec' => {version => 2},
            repository  => {
                url         => 'git://github.com:scottw/perl-anyevent-nsq.git',
                web         => 'http://github.com/scottw/perl-anyevent-nsq',
                type        => 'git',
            },
        },
    },
    dist                => {COMPRESS => 'gzip -9f', SUFFIX => 'gz',},
    clean               => {FILES    => 'anyevent-nsq-*',          },
);

