use warnings;
use strict;

# <?xml version="1.0"?><bulkwhois xmlns="http://www.arin.net/bulkwhois/core/v1">
# </bulkwhois>

#Split the file into 4 files. 
# 1) asn only file
# 2) poc only file
# 3) org only file
# 4) net only file
#

use constant {
    FL_START =>  '<?xml version="1.0"?><bulkwhois xmlns="http://www.arin.net/bulkwhois/core/v1">', 
    FL_END => '</bulkwhois>'
};

my $STATES = {
    'INIT' => 'INIT',
    'ASN' => 'ASN',
    'POC' => 'POC',
    'NET' => 'NET',
    'ORG' => 'ORG'
};

open IN, "<arin_db/arin_db.xml";

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
        #print "0\n";
        #Write the end of the previous file if nessessary.
        if($currentState ne $STATES->{'INIT'}) { 
            #print "1\n";
            print "Ending the $currentState file\n";
            print "Last line: $line\n\n";
            print $currOUT "\n\n".FL_END."\n\n";
            close $currOUT;
            $currOUT = undef;
#            exit;
        }
        
        if($newState ne $STATES->{INIT}) {
            #print "2\n";
            print "Starting the $newState file\n";
            print "File to write: arin_db_$newState.xml\n";
            print "First line: $line\n\n";
            $fileToWriteTo = "arin_db_$newState.xml";
            open OUT, ">>$fileToWriteTo";
            print OUT "\n\n".FL_START."\n\n";
            print OUT $line;
            $currOUT = *OUT;

            $currentState = $newState;
        }
    }
    #The state is the same print the line to the file.
    elsif($currentState ne $STATES->{'INIT'}) {
        print $currOUT $line;
    }
    elsif($currentState eq $STATES->{'INIT'}) {}
    else { 
        print "----------------Uncaught State------------------\n";
        print "Current State: $currentState\n";
        print "New State: $newState\n";
        print "Current Line: $line\n";
        print "Line Number: $lineNumber\n";
        print "------------------------------------------------\n";
        die;
    }

    if(($lineNumber % 50000) == 0) {
        print "Line number: $lineNumber\n";
        #exit if($lineNumber != 0);
    }
    $lineNumber++;
}

print "Total Lines: ". ($lineNumber + 1)."\n";

close(IN);


sub changeState {
    my $line = shift;

    my $state = 0;
    if($line =~ m/<asn>/) {
        $state = $STATES->{'ASN'};
    }
    elsif($line =~ m/<net>/) {
        $state = $STATES->{'NET'};
    }
    elsif($line =~ m/<poc>/) {
        $state = $STATES->{'POC'};
    }
    elsif($line =~ m/<org>/) {
        $state = $STATES->{'ORG'};
    }
    else {}

    return $state;
}




