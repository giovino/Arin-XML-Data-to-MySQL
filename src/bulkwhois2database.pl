#!/usr/bin/perl -w
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# bulkwhois2database.pl is a script that takes in a bulkwhois xml file and 
# dumps it to a database. For usage information go to the bottom of this file 
# or run ./bulkwhois2database --help.
#

use strict;
use warnings;
use Data::Dumper;
use XML::LibXML::Reader; #Read the file without using too much memory
use BulkWhois::Schema;
use InsertManager::XMLSimpleInsertManager;
use InsertManager::SAXInsertManager;
use Cwd;
use Scalar::Util 'blessed';
use Getopt::Long;   #Used for processing arguments.
use Pod::Usage;     #Used to display usage information

#Right now the default key that XML::Simple usees for element text is #TEXT. 
use constant {
    ELEMENT_TEXT => '#TEXT'
};

#~~~~~~~~~~~~~~~~~~~~~~~~~~ GET ARGUMENTS ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
my $numArgs = @ARGV;
my $args = {
    'verbose'   => 0,
    'file'      => '',
    'dbms'      => '',
    'database'  => '',
    'user'      => '',
    'password'  => '',
    'host'      => '',
    'port'      => '',
    'help'      => '',
    'man'       => '',
    'buffer-size'   => ''
};
GetOptions( 'v|verbose=i'       => \$args->{'verbose'}, #accept only integer 
            'f|file=s'          => \$args->{'file'},    #accept only string
            'm|dbms=s'          => \$args->{'dbms'},
            'd|database=s'      => \$args->{'database'},
            'u|user=s'          => \$args->{'user'},
            'p|password=s'      => \$args->{'password'},
            'h|host=s'          => \$args->{'host'},
            'g|port=i'          => \$args->{'port'},
            'help|?'            => \$args->{'help'},    #Treat as trigger only
            'man'               => \$args->{'man'},
            'buffer-size=i'     => \$args->{'buffer-size'}
        );
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

if (($numArgs == 0) && (-t STDIN)) {
    pod2usage("$0: Incorrect usage. Use the --help arguement for help"); 
}
elsif($args->{'help'} || $args->{'man'}) {
    pod2usage(1);
}
else {
    #Create a BulkWhois::Schema object (which inherits from DBIx::Class::Schema).
    my $dsn = "dbi:$args->{'dbms'}:$args->{'database'}:$args->{'host'}:$args->{'port'}";
    my $bulkWhoisSchema = BulkWhois::Schema->connect(
                            $dsn, 
                            $args->{'user'}, 
                            $args->{'password'}
                        ) or die "Failed to connect to database", DBIx->errstr;
    #Drop all of the tables from the database and recreate them
    dPrintln("Connecting to database. Displaying connection string:", $args->{'verbose'}, 1);
    dPrintln("Dropping all tables and starting fresh\n", $args->{'verbose'}, 1);
    my $connResults = $bulkWhoisSchema->deploy({add_drop_table => 1});  

    #    $connResults will always have a false value because the developers decided so.
    #    http://lists.scsys.co.uk/pipermail/dbix-class/2009-June/007963.html 
    #    The link above is a response to this issue.

    #Verbose statements 
    dPrintln("\t"       . $dsn, $args->{'verbose'}, 1);
    dPrintln("\tUser: "   . $args->{'user'}, $args->{'verbose'}, 1);
    dPrintln(($args->{'password'}) ? "\tPassword: yes" : "\tPassword: no", $args->{'verbose'}, 1); 

    #Set up the insertManager
    my $bufferSize = ($args->{'buffer-size'}) ? $args->{'buffer-size'} : 4095;
    dPrintln("Setting up an InsertManager object with a default buffer size of $bufferSize",
                $args->{'verbose'}, 1);
    my $insertManager = InsertManager::XMLSimpleInsertManager->new(bufferSize => $bufferSize, schema => $bulkWhoisSchema);
    $insertManager->defaultElementTextKey(ELEMENT_TEXT);

    #begin parsing and dumping to database
    dPrintln("Begin feeding xml to InsertManager object", $args->{'verbose'}, 1);
    feedFileToInsertManager(
                    file => $args->{'file'}, 
                    insertManager => $insertManager,
                    verbose => $args->{'verbose'}
    );
}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# The feedFileToInsertManager reads a bulkwhois file and loops
#  through all of the child elements of the <bulkwhois> element. 
#  For each child element it encounters will be extracted and passed
#  the InsertManager object. The object then takes the xml, parses 
#  it, and pushes it into a database.
#
#  This function expects all of its parameters
#  to be passed in as key => value pairs
#   @param file => the file to parse and dump to the database.
#   @param insertManager => an object that implements the InsertManagerInterface 
#   @param @optional verbose => 'boolean value' 1 or 0 to turn on or off verbal mode.
#
sub feedFileToInsertManager {

    #Initialize variables.
    my %args        = @_;
    my $verbose     = ($args{'verbose'}) ? $args{'verbose'} : 0;
    my $insertManager   = ($args{'insertManager'}) ? $args{'insertManager'} : die "I need an object that implements the InsertManagerInterface. insertManager => an object.\n";
    my $file    = ($args{'file'}) ? $args{'file'} : die "I need a file to parse.\n";
 
    dPrintln("Entered: ".(caller(0))[3], $verbose, 3); #get the name of this function.

    #Make sure the file path is valid. If it is then initialize an XML::LibXML::Reader 
    # object.
    dPrintln("Checking the file's path", $verbose, 2);
    my $xmlReader = (fileExists($file)) 
                    ? XML::LibXML::Reader->new(
                                    'location' => $file, 
                                    'load_ext_dtd' => 0
                                    )
                    : die $file . " is an invalid path\n";

    #Get line count for performance & measurements
    dPrintln("Calculating lines", $verbose, 1);
    my ($totalLines, $deltaTime) = ($verbose >= 1) ? countLinesInFile($file) : 0;
    dPrintln("Finished calculating lines", $verbose, 1);
    my $counter = 0;
    my $refreshRate = (($totalLines / 10000) < 1) ? 1 : int($totalLines / 10000);
    dPrintln("Time to count lines: $deltaTime seconds", $verbose, 2);
    dPrintln("Lines counted: $totalLines\n", $verbose, 2);
    dPrintln("Refresh every $refreshRate lines parsed", $verbose, 2);

    #Loop through the contents of the .xml file. Store all of the elements into the 
    #database.
    dPrintln("Let the feeding begin", $verbose, 1); 
    my $startTime = time; #Start the stopwatch
    my $sT = time;  #Used to tell you the time between a refresh.
    my $previousCounter = 0;
    while($xmlReader->read()) {
        #Only work on child elements of the root element <bulkwhois>
        if(($xmlReader->depth > 0) &&
            ($xmlReader->nodeType() != XML_READER_TYPE_SIGNIFICANT_WHITESPACE)) {

            #Print the progress to the screen.
            if(($counter % $refreshRate) == 0) { 
                dPrintln("Iteration: $counter\n", $verbose, 2);
            }
            if(!($counter % $refreshRate) && ($verbose >= 1)) {
                my $dT = time - $sT;
                $sT = time;
                my $dCounter = $counter - $previousCounter;
                $previousCounter = $counter;
                my $percentComplete = int((($xmlReader->lineNumber()) / $totalLines) * 100);
                print "$percentComplete% of the file has been processed.\n";
                print "\tIt took $dT seconds to parse $dCounter elements (asn, org, poc, or net).\n";
                print "\t". ($totalLines - $xmlReader->lineNumber) ." lines left to parse.\n";
            } 
            
            $insertManager->parseXML($xmlReader->readOuterXml(), $xmlReader->name); 

            $xmlReader->next();
            $counter++;
        }#END IF
    }#END WHILE
    $insertManager->endParsing; #perform some additional work (if needed)

    my $endTime = time;
    $deltaTime = $endTime - $startTime;
    dPrintln("$deltaTime seconds was required to parse the XML file\n", $verbose, 1);    
}#END feedFileToInsertManager


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Checks for the existance of a file based on the path given. If the 
# file exists then 1 is returned. Otherwise return 0.
#
#   @param the path of the file as a string.
#
sub fileExists {
    my $path = shift;
    
    return (-e $path) ? 1 : 0;
}


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Count the total number of lines in the file.
#
#   @param the path of the file to count.
#
sub countLinesInFile {
    my $path = shift;
    

    my $totalLines = 0;
    
    my $startTime = time;
    if(fileExists($path)) {    
        open(XML, "<$path");
        while(<XML>) {
            $totalLines++;
        }
        close(XML);
    }
    my $endTime = time;
    my $deltaTime = $endTime - $startTime;

    return ($totalLines, $deltaTime);
}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Print a line depending on verbosity level
#
#   @param the string to print.
#   @param the current debug level.
#   @param the minimum level to trigger a print event.
sub dPrintln {
    my $line = shift;
    my $currDebugLevel = shift;
    my $minDebugLevel = shift;
    
    print $line."\n" if($currDebugLevel >= $minDebugLevel);
}

__END__


=head1 NAME
    
   bulkwhois2database - A script that takes in a BulkWhois.xml file from ARIN
   and dumps it to the specified database. 

=head1 SYNOPSIS
    
bulkwhois2database  [--file FILE] [--dbms STRING] [--database STRING] 
                    [--host HOST_ADDRESS] [--port PORT_NUMBER] 
                    [--user USER_NAME] [--password PASSWORD]

bulkwhois2database  [--file FILE] [--dbms STRING] [--database STRING] 
                    [--host HOST_ADDRESS] [--port PORT_NUMBER] 
                    [--user USER_NAME] [--password PASSWORD] 
                    [optional arguments ...]

EXAMPLE:
    ./bulkwhois2database.pl --file ../arin_db.xml --dbms mysql 
                            --database BulkWhois --host localhost --port 3306 
                            --user root --password 12345 --verbose 1

=head1 OPTIONS

=over 8

=item B<--help>
    
    Print usage information to the screen.

=item B<--man>
    
    Print the man page to the screen.

=item B<-v NUM, --verbose NUM>
    
    Set the verbosity (debugging) level. This argument is optional. 

=item B<-f FILE, --file FILE> 
    
    The name of the file that contains the bulkwhois xml data.

=item B<-m STRING, --dbms STRING>
    
    The name of the database management system. For example if using MySQL then
    use --dbms mysql or -m mysql.

=item B<-d STRING, --database STRING>
    
    The name of the database to use. 

=item B<-u USER_NAME, --user USER_NAME>
    
    The user to log in as.

=item B<-p PASSWORD, --password PASSWORD>

    The password to use.

=item B<-h HOST_ADDRESS, --host HOST_ADDRESS>
    
    The address of the host with the database management system.

=item B<-g PORT_NUMBER, --port PORT_NUMBER>

    The port to connect to.

=item B<--buffer-size NUMBER>

    Set the maximum buffer size before performing a bulk dump to the database.
    This parameter is optional. If nothing is passed in the buffer size 
    defaults to 4095

=back

=head1 DESCRIPTION
    
    bulkwhois2database takes in an ARIN BulkWhois xml file and dumps it to the 
    specified database. Beware! When the file is dumped to a database all of 
    the previous tables will be dropped and repopulated with the new data.

=cut


