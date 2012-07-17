#!/usr/bin/perl
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# This perl script reads an xml file and purges the contents into
# a temporary  MySQL database. Once the database has been created 
# the script performs a second pass through and creates a permanent
# MySQL database in 3rd normal form (or at least makes it's best 
# attempt)
#
# TODO Add command line arguments to the application.
#   -v, --verbose   : same as debugging.
#   -f, --file      : the xml file to parse and insert into the database.
#   -m, --dbms      : the database management system to connect to.
#   -d, --database  : the databse to connect to.
#   -u, --user      : the username to connect as.
#   -p, --password  : the password of the user. passing in p will prompt for a password.
#   -h, --host      : the address of the dbms host.
#   -g, --port      : the port to connect to. Why use -g instead of -H? Because Getopt is case insensitive. '-g' stands for gate.
#   --help          : print usage information to the screen.
# TODO Convert ArinXMLParser to an executable script.
# TODO Convert the documentation to pad for all of the scripts

use strict;
use warnings;
use Data::Dumper;
use XML::LibXML::Reader; #Read the file without using too much memory
use BulkWhois::Schema;
use InsertManager::XMLSimpleInsertManager;
use InsertManager::SAXInsertManager;
use Cwd;
use Scalar::Util 'blessed';
use Getopt::Long; #Used for processing arguments.

#Right now the default key that XML::Simple usees for element text is #TEXT. 
use constant {
    ELEMENT_TEXT => '#TEXT'
};

#~~~~~~~~~~~~~~~~~~~~~~~~~~ GET ARGUMENTS ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Hash to store all of the arguments.
my $args = {
    'verbose'   => '',
    'file'      => '',
    'dbms'      => '',
    'database'  => '',
    'user'      => '',
    'password'  => '',
    'host'      => '',
    'port'      => '',
    'help'      => ''
};
GetOptions( 'v|verbose=i'   => \$args->{'verbose'},      #accept only integer 
            'f|file=s'      => \$args->{'file'},        #accept only string
            'm|dbms=s'      => \$args->{'dbms'},
            'd|database=s'  => \$args->{'database'},
            'u|user=s'      => \$args->{'user'},
            'p|password=s'  => \$args->{'password'},
            'h|host=s'      => \$args->{'host'},
            'g|port=i'      => \$args->{'port'},
            'help|?'        => \$args->{'help'}           #Treat as trigger only
        );
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
print Dumper $args;

#If the 'help' variable has been set then display usage information
#Otherwise begin parsing the document
if($args->{'help'}) {
    #Print usage information
}
else {
    #Create a BulkWhois::Schema object (which inherits from DBIx::Class::Schema).
    my $dsn = "dbi:$args->{'dbms'}:$args->{'database'}:$args->{'host'}:$args->{'port'}";
    my $bulkWhoisSchema = BulkWhois::Schema->connect($dsn, $args->{'user'}, $args->{'password'});
    
    $bulkWhoisSchema->deploy({add_drop_table => 1}); #Drop all of the tables from the database and recreate them

    #Set up the insertManager
    my $insertManager = InsertManager::XMLSimpleInsertManager->new(bufferSize => 65535, schema => $bulkWhoisSchema);
    $insertManager->defaultElementTextKey(ELEMENT_TEXT);

    #begin parsing and dumping to database
#    dumpXMLToSQLDB(
#                    file => $args->{'file'}, 
#                    insertManager => $insertManager,
#                    verbose => 1, debug => 0
#    );
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
#   @param @optional debug => 'boolean value' 1 or 0 to turn on or off debug mode.
sub dumpXMLToSQLDB {
    my $thisFunction = (caller(0))[3]; #get the name of this function.

    #Initialize variables.
    my %args        = @_;
    my $debug       = ($args{'debug'}) ? $args{'debug'} : 0;
    my $verbose     = ($args{'verbose'}) ? $args{'verbose'} : 0;
    my $insertManager   = ($args{'insertManager'}) ? $args{'insertManager'} : die "I need an object that implements the InsertManagerInterface. insertManager => an object.\n";
    my $file    = ($args{'file'}) ? $args{'file'} : die "I need a file to parse.\n";
    print "Done getting args\n" if($debug);

    #Make sure the file path is valid. If it is then initialize an XML::LibXML::Reader 
    # object.
    print "Checking the file's path\n" if($debug);
    my $xmlReader = (fileExists($file)) 
                    ? XML::LibXML::Reader->new(
                                    'location' => $file, 
                                    'load_ext_dtd' => 0
                                    )
                    : die $file . " is an invalid path\n";

    #Count the number of lines in the file for a progress report.
    #Set the refresh rate afterwards. This will print an update of 
    #the reading progress for 400 times throughout the dumping.
    print "Calculating lines\n" if($debug || $verbose);
    my ($totalLines, $deltaTime) = ($verbose) ? countLinesInFile($file) : 0;
    print "Finished calculating lines\n" if($debug || $verbose);
    my $counter = 0;
    my $refreshRate = (($totalLines / 10000) < 1) ? 1 : int($totalLines / 10000);
    print "Time to count lines: $deltaTime seconds\n" if($debug);
    print "Lines counted: $totalLines\n" if($debug);
    print "Refresh every $refreshRate lines parsed\n" if($debug); 

    #Loop through the contents of the .xml file. Store all of the elements into the 
    #database.
    print "Begin reading\n" if($debug); 
    my $startTime = time; #Start the stopwatch
    my $sT = time;  #Used to tell you the time between a refresh.
    my $previousCounter = 0;
    while($xmlReader->read()) {
        #Go through all of the child elements of the root node. Use XML::Simple
        # to convert them into a hash. Then go through the hash and push it into 
        # the database.
        if(($xmlReader->depth > 0) &&
            ($xmlReader->nodeType() != XML_READER_TYPE_SIGNIFICANT_WHITESPACE)) {

            #Print the progress to the screen.
            if(!($counter % $refreshRate) && $verbose) {
                my $dT = time - $sT;
                $sT = time;
                my $dCounter = $counter - $previousCounter;
                $previousCounter = $counter;
                my $percentComplete = int((($xmlReader->lineNumber()) / $totalLines) * 100);
                print "$percentComplete% of the file has been processed.\n";
                print "     It took $dT seconds to parse $dCounter elements (asn, org, poc, or net).\n";
            } 
            
            $insertManager->parseXML($xmlReader->readOuterXml(), $xmlReader->name); 

            $xmlReader->next();
            $counter++;
            do{ 
                print "Iteration: $counter\n" if($debug);
            } if(($counter % 1000) == 0);
        }#END IF
    }#END WHILE
    $insertManager->endParsing; #perform some additional work (if needed)

    my $endTime = time;
    $deltaTime = $endTime - $startTime;
    print "$deltaTime seconds was required to parse the XML file\n" if($verbose || $debug);    
}


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Checks for the existance of a file based on the path given. If the 
# file exists then 1 is returned. Otherwise return 0.
#   @param the path of hte file as a string.
sub fileExists {
    my $path = shift;
    
    return (-e $path) ? 1 : 0;
}


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Count the total number of lines in the file.
#
#   @param the path of the file to count.
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




