#!/usr/bin/env perl

use strict;
use warnings FATAL => 'all';

BEGIN {
    use Config;
    if (! $Config{'useithreads'}) {
        print("1..0 # SKIP Perl not compiled with 'useithreads'\n");
        exit(0);
    }
    if ($ENV{HARNESS_ACTIVE} && $ENV{HARNESS_PERL_SWITCHES} && $ENV{HARNESS_PERL_SWITCHES} =~ /Devel::Cover/) {
        print("1..0 # SKIP Cannot run tests that use threads when Devel::Cover is enabled\n");
        exit(0);
    }
}

use threads;
use Thread::Queue::Priority;

if ($] == 5.008) {
    require 't/test.pl';   # Test::More work-alike for Perl 5.8.0
} else {
    require Test::More;
}
Test::More->import();
plan('tests' => 19);

my $q = Thread::Queue::Priority->new();
$q->enqueue($_) for (1 .. 10);
ok($q, 'New queue');

$q->enqueue($_) for ([ qw/foo bar/ ]);

sub q_check
{
    is($q->peek(3), 4, 'Peek at queue');
    is($q->peek(-3), 9, 'Negative peek');

    my $nada = $q->peek(20);
    ok(! defined($nada), 'Big peek');
    $nada = $q->peek(-20);
    ok(! defined($nada), 'Big negative peek');

    my $ary = $q->peek(-1);
    is_deeply($ary, [ qw/foo bar/ ], 'Peek array');

    is($q->pending(), 11, 'Queue count in thread');
}

threads->create(sub {
    q_check();
    threads->create('q_check')->join();
})->join();
q_check();

