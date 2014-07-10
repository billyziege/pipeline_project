#!/usr/bin/perl -w
use strict;
use POSIX;
use List::Util;

my $vcf_header = "CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT";
my $vcf_file = shift;
my $dbsnp_vcf = '/mnt/speed/qc/sequencing/biodata/genomes/Hsapiens/GRCh37/variation/dbsnp_137.vcf';

(open DB,"<",$dbsnp_vcf)
    || (die "Couldn't open $dbsnp_vcf: $!");
my $chr_index = &get_index($vcf_header,"CHROM");
my $pos_index = &get_index($vcf_header,"POS");
my $struct = &read_chr_pos_from_vcf($vcf_file);
my $count = &count_autosomal_variants($struct);
my $in_dbsnp = 0;
while (<DB>)
{
    chomp;
    next if (($_ eq "") || (substr($_,0,1) eq "#"));
    my @columns = split(/\t/);
    my $chr = $columns[$chr_index];
    $chr =~ s/chr//;
    next unless ($chr =~ /^\d+$/);
    my $pos = $columns[$pos_index];
    $in_dbsnp++ if (defined($struct->{$chr}{$pos}) && ($struct->{$chr}{$pos} == 1) );
    $struct->{$chr}{$pos} = 0;
}
(close DB)
    || (warn "Couldn't close $dbsnp_vcf: $!");

print "in_dbsnp\ttotal_variants\tpercentage\n";

my $percentage;
if ($count != 0)
{
    $percentage = $in_dbsnp/$count*100;
}
else
{
    $percentage = 0;
}
printf("%d\t%d\t%.2f\n",$in_dbsnp,$count,$percentage);



sub read_chr_pos_from_vcf
{
    my $vcf_file = shift;
    (open IN,"<",$vcf_file)
        || (die "Couldn't open $vcf_file: $!");
    my $vcf_header = "CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT";
    my $chr_index = &get_index($vcf_header,"CHROM");
    my $pos_index = &get_index($vcf_header,"POS");
    my $struct;
    while (<IN>)
    {
        chomp;
        next if (($_ eq "") || (substr($_,0,1) eq "#"));
        my @columns = split(/\t/);
        my $chr = $columns[$chr_index];
        $chr =~ s/chr//;
        my $pos = $columns[$pos_index];
        $struct->{$chr}{$pos} = 1;
    }
    (close IN)
        || (warn "Couldn't close $vcf_file: $!");
    return $struct;
}

sub count_autosomal_variants
{
    my $struct = shift;
    my $count = 0;
    foreach my $chr (keys %{$struct})
    {
        next unless $chr =~ m/^\d+$/;
        foreach my $pos (keys %{$struct->{$chr}})
        {
            $count++;
        }
    }
    return $count;
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
    my ($call,$ref,$alt) = @_;
    (return $ref) if ($call eq "0");
    return $alt;
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
