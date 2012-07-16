#!/usr/bin/perl
#Splits the arin bulkwhois xml file into smaller xml files that
#only contain the elements asn, poc, net, and org.

use warnings;
use strict;
use Getopt::Long;
use Data::Dumper;
use Pod::Usage;
use Cwd;

my $args = {
    'verbose'   => 0,
    'help'      => '',
    'infile'    => '',
    'outfiles'  => ''
};

GetOptions ('verbose+'  => \$args->{'verbose'}, 
            'help'      => \$args->{'help'},
            'infile=s'  => \$args->{'infile'},
            'outfiles=s' => \$args->{'outfiles'}
            );

if($args->{'help'}) {
    print "---------------------------------------------------\n";
    print "Splitter takes in a arin bulkwhois xml file and creates four smaller files that only contain asn, org, poc, or net elements.\n";
    print "--verbose = tell the script to display parsing information. Repeat this command to increase the verbosity of the script\n";
    print "--help = print usage informaiton\n";
    print "--infile = the location of the bulk whois file to parse\n";
    print "--outfiles = the name of the splitted file. The format is outfilename_[element name].\n";
    print "\t For example running ".'"./Splitter --infile bulkwhois.xml --outfiles outs"'." will produce the following files: outs_asn.xml, outs_net.xml, outs_poc.xml, outs_org.xml\n";
    print "---------------------------------------------------\n";
    exit;
}

use constant {
    FL_START =>  '<?xml version="1.0"?><bulkwhois xmlns="http://www.arin.net/bulkwhois/core/v1">', 
    FL_END => '</bulkwhois>',
    REFRESH_RATE => 50000,
    STATES => {
        'INIT' => 'INIT',
        'ASN' => 'ASN',
        'POC' => 'POC',
        'NET' => 'NET',
        'ORG' => 'ORG'
    } 
};

my $STATES = {
    'INIT' => 'INIT',
    'ASN' => 'ASN',
    'POC' => 'POC',
    'NET' => 'NET',
    'ORG' => 'ORG'
};

#BEGIN splitting
open IN, "<".$args->{'infile'};

my $currentElement = '';
my $fileToWriteTo = '';
my $lineNumber = 0;
my $currentState = $STATES->{'INIT'};
my $currOUT = undef;

while (my $line = <IN>) {
    my $newState = changeState($line);

    #The state has changed
    #print "Current State: $currentState\n";
    #print "New State: $newState\n";
    if($newState && ($newState ne $currentState)) {
        #Write the end of the previous file if nessessary.
        if($currentState ne STATES->{'INIT'}) { 
            #print "1\n";
            if($args->{'verbose'} >= 1) {
                print "Ending the $fileToWriteTo\n";
                print "Last line: $line\n\n";
            }
            print $currOUT "\n\n".FL_END."\n\n";
            close $currOUT;
            $currOUT = undef;
        }
        
        if($newState ne STATES->{INIT}) {
            $fileToWriteTo = $args->{'infile'}.$newState.".xml";
            if($args->{'verbose'} >= 1) {
                print "Starting the $newState file\n";
                print "File to write: $fileToWriteTo\n";
                print "First line: $line\n\n";
            }
            open OUT, ">>$fileToWriteTo";
            print OUT FL_START."\n\n";
            print OUT $line;
            $currOUT = *OUT;

            $currentState = $newState;
        }
    }
    #The state is the same print the line to the file.
    elsif($currentState ne STATES->{'INIT'}) {
        print $currOUT $line;
    }
    elsif($currentState eq STATES->{'INIT'}) {}
    elsif($args->{debug}) { 
        print "----------------Uncaught State------------------\n";
        print "Current State: $currentState\n";
        print "New State: $newState\n";
        print "Current Line: $line\n";
        print "Line Number: $lineNumber\n";
        print "------------------------------------------------\n";
        die;
    }

    if((($lineNumber % REFRESH_RATE) == 0) && ($args->{'verbose'} >= 2)) {
        print "Line number: $lineNumber\n";
    }
    $lineNumber++;
}

print "Total Lines: ". ($lineNumber + 1)."\n" if($args->{'verbose'} >= 1);

close(IN);
#END splitting


sub changeState {
    my $line = shift;

    my $state = 0;
    if($line =~ m/<asn>/) {
        $state = STATES->{'ASN'};
    }
    elsif($line =~ m/<net>/) {
        $state = STATES->{'NET'};
    }
    elsif($line =~ m/<poc>/) {
        $state = STATES->{'POC'};
    }
    elsif($line =~ m/<org>/) {
        $state = STATES->{'ORG'};
    }
    else {}

    return $state;
}


##### SCRAP ######

#print Dumper $args;

#my $infile = $args->{'infile'};

#while($infile =~ /\/?([\w\s\-]+)\/{1}/g) { 
#    print "Path: $1\n";
#}

#exit;

# <?xml version="1.0"?><bulkwhois xmlns="http://www.arin.net/bulkwhois/core/v1">
# </bulkwhois>

#Split the file into 4 files. 
# 1) asn only file
# 2) poc only file
# 3) org only file
# 4) net only file
#

