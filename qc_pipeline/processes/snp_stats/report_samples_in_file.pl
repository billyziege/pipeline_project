#!/usr/bin/perl -w
use strict;
use POSIX;
use List::Util;

my $file = shift;

(open IN,"<",$file)
    || (die "Couldn't open $file: $!");
my $line = <IN>;
#Print meta-info at top.
while (substr($line,0,1) eq "#")
{
    $line = <IN>;
}
my $header = $line;
chomp $header;
my @head = split(/\t/,$header);
shift @head;
foreach my $h (@head)
{
    $h =~ s/\s+//g;
    $h = substr($h,0,15);
}
print join(",",@head);

sub read_file_lines_as_array
{
    my $file = shift;
    (open IN,"<",$file)
        || (die "Couldn't open $file: $!");
    my @array = <IN>;
    (close IN) || (warn "Could not close $file: $!\n");
    return \@array;
}

sub copy_hash
{
    my ($from,$to) = @_;
    foreach my $k (keys %{$from})
    {
        $to->{$k} = $from->{$k};
    }
    return $to;
}

sub print_interval_and_target
{
    my $struct = shift;
    my $number = shift;
    print "chr".$struct->{chr}."\t".$struct->{start}."\t".$struct->{end}."\t+\ttarget_$number\n";
    return 1;
}

sub get_index
{
    my ($line,$name) = @_;
    my @columns = split(/\s+/,$line);
    my $i;
    foreach $i (0..$#columns)
    {	
        (return $i) if ($name eq $columns[$i]);
    }
    return -1;
}

sub load_variable_by_iid
{
    my $file = shift;
    my $variable = shift;
    (open IN, "<", $file) || 
        (die "Could not open $file :$!\n");
    my $header = <IN>;
    chomp $header;
    $header =~ s/"//g;
    my $iid_index = &get_index_csv($header,"DID_KIT_ID");
    my $var_index = &get_index_csv($header,$variable);
    my $hash;
    while (<IN>)
    {
        chomp;
        s/"//g;
        my @columns = split(/,/,$_);
        $hash->{$columns[$iid_index]} = $columns[$var_index];
    }
    (close IN) || 
        (warn "Could not close $file: $!\n");
    return $hash;
}
