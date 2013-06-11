#!/usr/bin/perl -w
use strict;
use POSIX;
use List::Util;

my $array_call_file = shift;
my $genotype_call_file = shift;
my $acalls = &read_genotype_calls($array_call_file);
my $gcalls = &read_genotype_calls($genotype_call_file);
my ($same,$total,$errors) = &concordance_of_intersect_calls($acalls,$gcalls);

print "same\ttotal\tpercentage\tformat\terrors\n";
my $percentage;
if ($total == 0)
{
    $percentage = 0;
}
else
{
    $percentage = $same*100/$total;
}
print $same;
print "\t".$total;
print "\t$percentage";
print "\t";
my @names = keys %{$errors};
foreach my $i (0..$#names)
{
    (print ":") unless ($i == 0);
    print $names[$i];
}
print "\t";
foreach my $i (0..$#names)
{
    (print ":") unless ($i == 0);
    print $errors->{$names[$i]};
}
print "\n";


sub concordance_of_intersect_calls
{
    my ($struct1,$struct2) = @_;
    my $total = 0;
    my $same = 0;
    my $errors;
    foreach my $chr (keys %{$struct1})
    {
        next unless defined($struct2->{$chr});
        foreach my $pos (keys %{$struct1->{$chr}})
        {
            next unless defined($struct2->{$chr}{$pos});
            next if ( ($struct2->{$chr}{$pos} =~ m/0/) || ($struct2->{$chr}{$pos} =~ m/\./) || ($struct2->{$chr}{$pos} =~ m/-/) );
            next if ( ($struct1->{$chr}{$pos} =~ m/0/) || ($struct1->{$chr}{$pos} =~ m/\./) || ($struct1->{$chr}{$pos} =~ m/-/) );
            $total++;
            if ($struct1->{$chr}{$pos} eq $struct2->{$chr}{$pos})
            {
                $same++;
            }
            else
            {
                my $error = $struct1->{$chr}{$pos}."-->".$struct2->{$chr}{$pos};
                ($errors->{$error} = 0) unless (defined($errors->{$error}));
                $errors->{$error}++;
            }
            
        }
    }
    return ($same,$total,$errors);
}

sub read_genotype_calls
{
    my $file = shift;
    (open IN,"<",$file)
        || (die "Couldn't open $file: $!");
    my $hash;
    while (<IN>)
    {
        chomp;
        my ($chr,$pos,@calls) = split(/\t/);
        foreach my $i (0..$#calls)
        {
            $hash->{$chr}{$pos} = &alphabetize_calls($calls[$i]);
        }
    }
    (close IN)
        || (warn "Couldn't close $file: $!");
    return $hash;
}

sub alphabetize_calls
{
    my $call = shift;
    my @as = split(/\//,$call);
    my @sas = sort @as;
    my $acall = join("/",@sas);
    return $acall;
}

sub same_call
{
    my ($c1,$c2) = @_;
    my $a11 = substr($c1,0,1);
    my $a12 = substr($c1,2,1);
    my $a21 = substr($c2,0,1);
    my $a22 = substr($c2,2,1);
    #die "$a11,$a12:$a21,$a22\n";
    #print $c2."\n";
    #(print $c2."\n") if (length($c2) > 2);
    (return "True") if ( ($a11 eq $a21) && ($a12 eq $a22) );
    (return "True") if ( ($a12 eq $a21) && ($a11 eq $a22) );
    my %bases;
    $bases{"A"} = 1;
    $bases{"C"} = 1;
    $bases{"G"} = 1;
    $bases{"T"} = 1;
    (die "The call $c1, or $a11 and $a12, is not a base.\n") unless ( defined($bases{$a11}) &&
        defined($bases{$a12}) );
    (die "The call $c2, or $a21 and $a22, is not a base.\n") unless ( defined($bases{$a21}) &&
        defined($bases{$a22}) );
    return "False";
}

sub read_map_snps_as_array
{
    my $file = shift;
    (open IN,"<",$file)
        || (die "Couldn't open $file: $!");
    my $snps;
    my $count = 0;
    while (<IN>)
    {
        chomp;
        my ($chr,$snp,$cm,$pos) = split(/\t/);
        $snps->[$count]{"chr"}="chr".$chr;
        $snps->[$count]{"pos"} =$pos;
        $count++;
    }
    (close IN)
        || (warn "Couldn't close $file: $!");
    return $snps;
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
