#!/usr/bin/perl -w
use strict;
use POSIX;
use List::Util;

my $call_file = shift;
my $bed_file = shift;
my $filter_positions = &read_in_bed_pos($bed_file);
my $snp_lookup_file = '/home/zerbeb/homemade_programs/sequence_analysis/combined_GO_annotations_v8.tsv';
my $snp_lookup_table = &load_in_filtered_snp_by_probeset_id
    ($snp_lookup_file,$filter_positions);

(open IN,"<",$call_file)
    || (die "Couldn't open $call_file: $!");
my $struct;
while (<IN>)
{
    chomp;
    if (substr($_,0,1) eq "#") 
    {
        next;
    }
    my @columns = split(/\s+/);
    if ($columns[0] eq "probeset_id") 
    {
        shift @columns;
        print "chr\tpos\t".join("\t",@columns)."\n";
        next;
    }
    my $id = shift @columns;
    next unless (defined($snp_lookup_table->{$id}));
    my $chr = $snp_lookup_table->{$id}{'chr'};
    my $pos = $snp_lookup_table->{$id}{'pos'};
    my $alleles = $snp_lookup_table->{$id}{'alleles'};
    $chr =~ s/chr//;
    foreach my $i (0..$#columns)
    {
        my $geno = &translate_call($columns[$i],$snp_lookup_table->{$id}{'alleles'});
        $struct->{$chr}{$pos}[$i] = $geno;
    }
}
(close IN)
    || (warn "Couldn't close $call_file: $!");

foreach my $chr (sort {$a<=>$b} keys %{$struct})
{
    foreach my $pos (sort {$a<=>$b} keys %{$struct->{$chr}})
    {
        print "chr$chr\t$pos";
        foreach my $i (0..$#{$struct->{$chr}{$pos}})
        {
            print "\t".$struct->{$chr}{$pos}[$i];
        }
        print "\n";
    }
}

sub load_in_filtered_snp_by_probeset_id
{
    my ($file,$filter) = @_;
    (open IN,"<",$file)
        || (die "Couldn't open $file: $!");
    my $header = <IN>;
    chomp $header;
    my $hash;
    my $chr_index = &get_index($header,'affy_b37_chr');
    my $pos_index = &get_index($header,'affy_b37_pos');
    my $id_index = &get_index($header,'probeset_id');
    my $a_index = &get_index($header,'affy_b37_allele_A');
    my $b_index = &get_index($header,'affy_b37_allele_B');
    while (<IN>)
    {
        chomp;
        my @columns = split(/\t/);
        my $chr = "chr".$columns[$chr_index];
        my $pos = $columns[$pos_index];
        next unless (defined($filter->{$chr}) && defined($filter->{$chr}{$pos}));
        my $id = $columns[$id_index];
        my $a = $columns[$a_index];
        my $b = $columns[$b_index];
        $hash->{$id}{"chr"} = $chr;
        $hash->{$id}{"pos"} = $pos;
        $hash->{$id}{"alleles"} = "$a/$b";
    }
    (close IN)
        || (warn "Couldn't close $file: $!");
    return $hash;
}

sub get_alphabetized_alleles
{
    my $line = shift;
    my $vcf_header = "CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT";
    my $ref_index = &get_index($vcf_header,"REF");
    my $alt_index = &get_index($vcf_header,"ALT");
    my $format_index = &get_index($vcf_header,"FORMAT");
    my @columns = split(/\t/,$line);
    my @individuals = @columns[9..$#columns];
    my $ref = $columns[$ref_index];
    my $alt = $columns[$alt_index];
    my $format = $columns[$format_index];
    my $gt_index = &get_index_colon($format,"GT");
    my $alleles;
    foreach my $ind (0..$#individuals)
    {
        my @vals = split(/:/,$individuals[$ind]);
        my $calls = $vals[$gt_index];
        my @cs = split(/\//,$calls);
        my @as;
        $as[0] = &translate_call($cs[0],$ref,$alt);
        $as[1] = &translate_call($cs[1],$ref,$alt);
        my @sas = sort @as;
        $alleles->[$ind][0] = $sas[0];
        $alleles->[$ind][1] = $sas[1];
    }
    return $alleles;
}

sub translate_call
{
    my ($call,$alleles) = @_;
    my ($a,$b) = split(/\//,$alleles);
    (return "$a/$a") if ($call eq "0");
    (return "$b/$b") if ($call eq "2");
    (return 0) if ($call eq ".");
    if ($call eq "1")
    {
        if ($a cmp $b)
        {
            return "$a/$b";
        }
        return "$b/$a";
    }
    if ($call eq "-1")
    {
        return ".";
    }
    die "Incorrectly formatted call: $call\n";
}

sub read_in_ref
{
    my $file = shift;
    my $header = "CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT";
    my $chr_index = &get_index($header,"CHROM");
    my $pos_index = &get_index($header,"POS");
    my $snp_index = &get_index($header,"ID");
    my $hash;
    (open IN,"<",$file)
        || (die "Couldn't open $file: $!");
    while (<IN>)
    {
        next if (substr($_,0,1) eq '#');
        chomp;
        next if ($_ =~ m/$header/);
        my @columns = split(/\t/);
        my $chr = $columns[$chr_index];
        my $pos = $columns[$pos_index];
        my $snp = $columns[$snp_index];
        $hash->{$chr}{$pos} = $snp;
    }
    (close IN)
        || (warn "Couldn't close $file: $!");
    return $hash;
}

sub read_in_bed_pos
{
    my $file = shift;
    (open IN,"<",$file)
        || (die "Couldn't open $file: $!");
    my $hash;
    while (<IN>)
    {
        chomp;
        my ($chr,$pos_begin,$pos_end,@rest) = split(/\t/);
        my $pos = $pos_begin + 1;
        while ($pos <= $pos_end)
        {
            $hash->{$chr}{$pos} = 1;
            $pos++;
        }
    }
    (close IN)
        || (warn "Couldn't close $file: $!");
    return $hash;
}

sub get_index
{
    my ($line,$name) = @_;
    my @columns = split(/\t/,$line);
    my $i;
    foreach $i (0..$#columns)
    {	
        (return $i) if ($name eq $columns[$i]);
    }
    return -1;
}

sub get_index_colon
{
    my ($line,$name) = @_;
    my @columns = split(/:/,$line);
    my $i;
    foreach $i (0..$#columns)
    {	
        (return $i) if ($name eq $columns[$i]);
    }
    return -1;
}
sub average
{
    my @array = @_;
    my $i;
    my $sum = 0;
    my $count = $#array + 1;
    for $i (0..$#array)
    {
        $sum += $array[$i];
    }
    my $avg = $sum/$count;
    return $avg;
}

sub get_index_csv
{
    my ($line,$name) = @_;
    my @columns = split(/,/,$line);
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
    my $iid_index = &get_index($header,"IID");
    my $var_index = &get_index($header,$variable);
    my $hash;
    while (<IN>)
    {
        chomp;
        s/"//g;
        my @columns = split(/\t/,$_);
        $hash->{$columns[$iid_index]} = $columns[$var_index];
    }
    (close IN) || 
        (warn "Could not close $file: $!\n");
    return $hash;
}
