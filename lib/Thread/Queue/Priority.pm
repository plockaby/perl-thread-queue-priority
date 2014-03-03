package Thread::Queue::Priority;

use strict;
use warnings;

our $VERSION = '1.0.0';
$VERSION = eval $VERSION;

use Carp;
use threads::shared 1.21;
use Scalar::Util qw(looks_like_number);

sub new {
    my $class = shift;
    my %queue :shared = ();
    my %self :shared = (
        '_queue'   => \%queue,
        '_count'   => 0,
        '_ended'     => 0,
    );
    return bless(\%self, $class);
}

# add items to the tail of a queue
sub enqueue {
    my ($self, $item, $priority) = @_;
    lock(%{$self});

    # if the queue has "ended" then we can't enqueue anything
    croak("'enqueue' method called on queue that has been 'end'ed") if $self->{'_ended'};

    my $queue = $self->{'_queue'};
    $priority = defined($priority) ? _validate_priority($priority) : 50;

    # if the priority group hasn't been created then create it
    my @group :shared = ();
    $queue->{$priority} = \@group unless exists($queue->{$priority});

    # increase our global count
    ++$self->{'_count'};

    # add the new item to the priority list and signal that we're done
    push(@{$self->{'_queue'}->{$priority}}, shared_clone($item)) and cond_signal(%{$self});
}

# return a count of the number of items on a queue
sub pending {
    my $self = shift;
    lock(%{$self});

    # return undef if the queue has ended and is empty
    return if $self->{'_ended'} && !$self->{'_count'};
    return $self->{'_count'};
}

# indicate that no more data will enter the queue
sub end {
    my $self = shift;
    lock(%{$self});

    # no more data is coming
    $self->{'_ended'} = 1;

    # try to release at least one blocked thread
    cond_signal(%{$self});
}

# return 1 or more items from the head of a queue, blocking if needed
sub dequeue {
    my $self = shift;
    lock(%{$self});

    my $queue = $self->{'_queue'};
    my $count = scalar(@_) ? _validate_count(shift(@_)) : 1;

    # wait for requisite number of items
    cond_wait(%{$self}) while (($self->{'_count'} < $count) && ! $self->{'_ended'});
    cond_signal(%{$self}) if (($self->{'_count'} > $count) || $self->{'_ended'});

    # if no longer blocking, try getting whatever is left on the queue
    return $self->dequeue_nb($count) if ($self->{'_ended'});

    # return single item
    if ($count == 1) {
        for my $priority (sort keys %{$queue}) {
            if (scalar(@{$queue->{$priority}})) {
                --$self->{'_count'};
                return shift(@{$queue->{$priority}});
            }
        }
        return;
    }

    # return multiple items
    my @items = ();
    for (1 .. $count) {
        for my $priority (sort keys %{$queue}) {
            if (scalar(@{$queue->{$priority}})) {
                --$self->{'_count'};
                push(@items, shift(@{$queue->{$priority}}));
            }
        }
    }
    return @items;
}

# return items from the head of a queue with no blocking
sub dequeue_nb {
    my $self = shift;
    lock(%{$self});

    my $queue = $self->{'_queue'};
    my $count = scalar(@_) ? _validate_count(shift(@_)) : 1;

    # return single item
    if ($count == 1) {
        for my $priority (sort keys %{$queue}) {
            if (scalar(@{$queue->{$priority}})) {
                --$self->{'_count'};
                return shift(@{$queue->{$priority}});
            }
        }
        return;
    }

    # return multiple items
    my @items = ();
    for (1 .. $count) {
        for my $priority (sort keys %{$queue}) {
            if (scalar(@{$queue->{$priority}})) {
                --$self->{'_count'};
                push(@items, shift(@{$queue->{$priority}}));
            }
        }
    }

    return @items;
}

# return items from the head of a queue, blocking if needed up to a timeout
sub dequeue_timed {
    my $self = shift;
    lock(%{$self});

    my $queue = $self->{'_queue'};
    my $timeout = scalar(@_) ? _validate_timeout(shift(@_)) : -1;
    my $count = scalar(@_) ? _validate_count(shift(@_)) : 1;

    # timeout may be relative or absolute
    # convert to an absolute time for use with cond_timedwait()
    # so if the timeout is less than a year then we assume it's relative
    $timeout += time() if ($timeout < 322000000); # more than one year

    # wait for requisite number of items, or until timeout
    while ($self->{'_count'} < $count && !$self->{'_ended'}) {
        last unless cond_timedwait(%{$self}, $timeout);
    }
    cond_signal(%{$self}) if (($self->{'_count'} > $count) || $self->{'_ended'});

    # get whatever we need off the queue if available
    return $self->dequeue_nb($count);
}

# return an item without removing it from a queue
sub peek {
    my $self = shift;
    lock(%{$self});

    my $queue = $self->{'_queue'};
    my $index = scalar(@_) ? _validate_index(shift(@_)) : 0;

    for my $priority (sort keys %{$queue}) {
        my $size = scalar(@{$queue->{$priority}});
        if ($index < $size) {
            return $queue->{$priority}->[$index];
        } else {
            $index = ($index - $size);
        }
    }

    return;
}

### internal functions ###

# check value of the requested index
sub _validate_index {
    my $index = shift;

    if (!defined($index) || !looks_like_number($index) || (int($index) != $index)) {
        my ($method) = (caller(1))[3];
        $method =~ s/^Thread::Queue::Priority:://;
        $index = 'undef' unless defined($index);
        croak("Invalid 'index' argument (${index}) to '${method}' method");
    }

    return $index;
}

# check value of the requested count
sub _validate_count {
    my $count = shift;

    if (!defined($count) || !looks_like_number($count) || (int($count) != $count) || ($count < 1)) {
        my ($method) = (caller(1))[3];
        $method =~ s/^Thread::Queue::Priority:://;
        $count = 'undef' unless defined($count);
        croak("Invalid 'count' argument (${count}) to '${method}' method");
    }

    return $count;
}

# check value of the requested timeout
sub _validate_timeout {
    my $timeout = shift;

    if (!defined($timeout) || !looks_like_number($timeout)) {
        my ($method) = (caller(1))[3];
        $method =~ s/^Thread::Queue::Priority:://;
        $timeout = 'undef' unless defined($timeout);
        croak("Invalid 'timeout' argument (${timeout}) to '${method}' method");
    }

    return $timeout;
}

# check value of the requested timeout
sub _validate_priority {
    my $priority = shift;

    if (!defined($priority) || !looks_like_number($priority) || (int($priority) != $priority) || ($priority < 0)) {
        my ($method) = (caller(1))[3];
        $method =~ s/^Thread::Queue::Priority:://;
        $priority = 'undef' unless defined($priority);
        croak("Invalid 'priority' argument (${priority}) to '${method}' method");
    }

    return $priority;
}

1;

=head1 NAME

Thread::Queue::Priority - Thread-safe queues with priorities

=head1 VERSION

This document describes Thread::Queue::Priority version 1.0.0

=head1 SYNOPSIS

    use strict;
    use warnings;

    use threads;
    use Thread::Queue::Priority;

    # create a new empty queue with no max limit
    my $q = Thread::Queue::Priority->new();

    # add a new element with default priority 50
    $q->enqueue("foo");

    # add a new element with priority 1
    $q->enqueue("foo", 1);

    # dequeue the highest priority on the queue
    my $value = $q->dequeue();

=head1 DESCRIPTION

This is a variation on L<Thread::Queue> that will dequeue items based on their
priority.

=head1 SEE ALSO

L<Thread::Queue>, L<threads>, L<threads::shared>

=head1 MAINTAINER

Paul Lockaby S<E<lt>plockaby AT cpan DOT orgE<gt>>

=head1 CREDIT

Large huge portions of this module are directly from L<Thread::Queue> which is
maintained by Jerry D. Hedden.

=head1 LICENSE

This program is free software; you can redistribute it and/or modify it under
the same terms as Perl itself.

=cut
