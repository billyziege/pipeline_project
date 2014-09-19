#!/usr/bin/perl -w
use strict;
use POSIX;
use List::Util;

my $file = shift;
my $sample_list_file = shift;
my $sample_list = &read_file_lines_as_array($sample_list_file);

(open IN,"<",$file)
    || (die "Couldn't open $file: $!");
my $line = <IN>;
#Print meta-info at top.
while (substr($line,0,1) eq "#")
{
    print $line;
    $line = <IN>;
}
my $header = $line;
chomp $header;
my $pid_index = &get_index($header,"probeset_id");
my $sample_index;
foreach my $i (0..$#{$sample_list})
{
    $sample_index->[$i] = &get_index($header,$sample_list->[$i]);
    (die "Sample ".$sample_list->[$i]." is not in $file:\n") if
        ($sample_index->[$i] == -1);
}
print "probeset_id ".join(" ",@{$sample_list})."\n";

while(<IN>)
{
    chomp;
    my @columns = split(/\s+/);
    print $columns[$pid_index];
    foreach my $i (0..$#{$sample_index})
    {
        print " ";
        print $columns[$sample_index->[$i]];
    }
    print "\n";
}

sub read_file_lines_as_array
{
    my $file = shift;
    (open IN,"<",$file)
        || (die "Couldn't open $file: $!");
    my @array = <IN>;
    chomp @array;
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
