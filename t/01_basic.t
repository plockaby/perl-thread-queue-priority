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
plan('tests' => 81);

### Basic usage with multiple threads ###

my $nthreads = 5;

my $q = Thread::Queue::Priority->new();
$q->enqueue($_) for (1 .. $nthreads);

ok($q, 'New queue');
is($q->pending(), $nthreads, 'Pre-populated queue count');

sub reader {
    my $id = threads->tid();
    while ((my $el = $q->dequeue()) != -1) {
        ok($el >= 1, "Thread $id got $el");
        select(undef, undef, undef, rand(1));
    }
    ok(1, "Thread $id done");
}

my @threads;
push(@threads, threads->create('reader')) for (1..$nthreads);

for (1..20) {
    select(undef, undef, undef, rand(1));
    $q->enqueue($_);
}

$q->enqueue($_) for ((-1) x $nthreads);   # One end marker for each thread

$_->join() foreach @threads;
undef(@threads);

is($q->pending(), 0, 'Empty queue');


### ->dequeue_nb() test ###

$q = Thread::Queue::Priority->new();
ok($q, 'New queue');
is($q->pending(), 0, 'Empty queue');

my @items = qw/foo bar baz/;
$q->enqueue($_) for (@items);

threads->create(sub {
    is($q->pending(), scalar(@items), 'Queue count in thread');
    while (my $el = $q->dequeue_nb()) {
        is($el, shift(@items), "Thread got $el");
    }
    is($q->pending(), 0, 'Empty queue');
    $q->enqueue('done');
})->join();

is($q->pending(), 1, 'Queue count after thread');
is($q->dequeue(), 'done', 'Thread reported done');
is($q->pending(), 0, 'Empty queue');


### ->dequeue(COUNT) test ###

my $count = 3;

sub reader2 {
    my $id = threads->tid();
    while (1) {
        my @el = $q->dequeue($count);
        is(scalar(@el), $count, "Thread $id got @el");
        select(undef, undef, undef, rand(1));
        return if ($el[0] == 0);
    }
}

push(@threads, threads->create('reader2')) for (1..$nthreads);

$q->enqueue($_) for (1 .. (4 * $count * $nthreads));
$q->enqueue($_) for ((0) x ($count * $nthreads));

$_->join() foreach @threads;
undef(@threads);

is($q->pending(), 0, 'Empty queue');


### ->dequeue_nb(COUNT) test ###

@items = qw/foo bar baz qux exit/;
$q->enqueue($_) for (@items);
is($q->pending(), scalar(@items), 'Queue count');

threads->create(sub {
    is($q->pending(), scalar(@items), 'Queue count in thread');
    while (my @el = $q->dequeue_nb(2)) {
        is($el[0], shift(@items), "Thread got $el[0]");
        if ($el[0] eq 'exit') {
            is(scalar(@el), 1, 'Thread to exit');
        } else {
            is($el[1], shift(@items), "Thread got $el[1]");
        }
    }
    is($q->pending(), 0, 'Empty queue');
    $q->enqueue('done');
})->join();

is($q->pending(), 1, 'Queue count after thread');
is($q->dequeue(), 'done', 'Thread reported done');
is($q->pending(), 0, 'Empty queue');

