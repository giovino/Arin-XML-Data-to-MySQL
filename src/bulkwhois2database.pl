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
use Log::Log4perl qw(:easy);    #Simple and intuitive logging
#use Log::Log4perl::Appender::File;      #Used for logging to a file
#use Log::Log4perl::Appender::Screen;    #Used for logging to a screen
#use Log::Log4perl::Level;               #Constants $FATAL,...,$TRACE

#Right now the default key that XML::Simple usees for element text is #TEXT. 
use constant {
    ELEMENT_TEXT => '#TEXT'
};

#~~~~~~~~~~~~~~~~~~~~~~~~~~ GET ARGUMENTS ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
my $numArgs = @ARGV;
my $args = {
    'verbose'   => 0,
    'log'       => 0,
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
GetOptions( 'v|verbose=s'       => \$args->{'verbose'}, #accept only integer 
            'l|log=s'           => \$args->{'log'},
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

#Initialize a logger to the appropriate level.
my $logLevel = $OFF;
if($args->{'log'} && !$args->{'verbose'}) {
    print "You need to set the verbosity level to at least '1' if you are going
to use logging\n";
    print "type 'bulkwhois2database --help' for more information\n";
    exit;
}
elsif($args->{'log'} && $args->{'verbose'}) {
    Log::Log4perl->easy_init({ 
            level => stringForLog4PerlLevel($args->{'verbose'}),
            file => ">>".$args->{'log'}
        });
    $logLevel = stringForLog4PerlLevel($args->{'verbose'});
}
elsif($args->{'verbose'}) {    
    Log::Log4perl->easy_init(stringForLog4PerlLevel($args->{'verbose'}));
    $logLevel = stringForLog4PerlLevel($args->{'verbose'});
}
else {
    Log::Log4perl->easy_init($OFF);
}

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
    TRACE "Connecting to database"; 
    TRACE "Displaying connection string:", $dsn;
    TRACE "User: ", $args->{'user'}, ($args->{'password'}) ? "\tPassword: yes" : "\tPassword: no";
    my $connResults = $bulkWhoisSchema->deploy({add_drop_table => 1});  

    #    $connResults will always have a false value because the developers decided so.
    #    http://lists.scsys.co.uk/pipermail/dbix-class/2009-June/007963.html 
    #    The link above is a response to this issue.


    #Set up the insertManager
    my $bufferSize = ($args->{'buffer-size'}) ? $args->{'buffer-size'} : 4095;
    TRACE "Setting up an InsertManager object with a default buffer size of $bufferSize";
    my $insertManager = InsertManager::XMLSimpleInsertManager->new(bufferSize => $bufferSize, schema => $bulkWhoisSchema, logger => Log::Log4perl->get_logger);
    $insertManager->defaultElementTextKey(ELEMENT_TEXT);

    #begin parsing and dumping to database
    TRACE "Begin feeding xml to InsertManager object";
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
 
    DEBUG "Entered: ", (caller(0))[3]; #get the name of this function.

    #Make sure the file path is valid. If it is then initialize an XML::LibXML::Reader 
    # object.
    TRACE "Checking the file's path";
    my $xmlReader = (fileExists($file)) 
                    ? XML::LibXML::Reader->new(
                                    'location' => $file, 
                                    'load_ext_dtd' => 0
                                    )
                    : die $file . " is an invalid path\n";

    #Get line count for performance & measurements
    TRACE "Calculating lines";
    my ($totalLines, $deltaTime) = ($logLevel != $OFF) ? countLinesInFile($file) : (0, 0);
    TRACE "Finished calculating lines";
    my $counter = 0;
    my $refreshRate = (($totalLines / 10000) < 1) ? 1 : int($totalLines / 10000);
    DEBUG "Time to count lines: $deltaTime seconds";
    DEBUG "Lines counted: $totalLines";
    DEBUG "Refresh every $refreshRate lines parsed";

    #Loop through the contents of the .xml file. Store all of the elements into the 
    #database.
    TRACE "Let the feeding begin";
    my $startTime = time; #Start the stopwatch
    my $sT = time;  #Used to tell you the time between a refresh.
    my $previousCounter = 0;
    while($xmlReader->read()) {
        #Only work on child elements of the root element <bulkwhois>
        if(($xmlReader->depth > 0) &&
            ($xmlReader->nodeType() != XML_READER_TYPE_SIGNIFICANT_WHITESPACE)) {

            #Print the progress to the screen.
            if(!($counter % $refreshRate) && ($logLevel == $DEBUG)) { 
                DEBUG "Iteration: $counter";
            }
            if(!($counter % $refreshRate) && ($logLevel == $TRACE) || ($logLevel == $DEBUG)) {
                my $dT = time - $sT;
                $sT = time;
                my $dCounter = $counter - $previousCounter;
                $previousCounter = $counter;
                my $percentComplete = int((($xmlReader->lineNumber()) / $totalLines) * 100);
                TRACE "\t$percentComplete% of the file has been processed.";
                TRACE "\t\tIt took $dT seconds to parse $dCounter elements (asn, org, poc, or net).";
                TRACE "\t\t". ($totalLines - $xmlReader->lineNumber) ." lines left to parse.";
            } 
            
            $insertManager->parseXML($xmlReader->readOuterXml(), $xmlReader->name); 

            $xmlReader->next();
            $counter++;
        }#END IF
    }#END WHILE
    $insertManager->endParsing; #perform some additional work (if needed)

    my $endTime = time;
    $deltaTime = $endTime - $startTime;
    TRACE "$deltaTime seconds was required to parse the XML file";
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

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# String for Log::Log4perl::Level constant
#
# @param a string
#
# @return one of the logging constant values from Log::Log4perl::Level
#
sub stringForLog4PerlLevel {
    my $string = shift;

    return Log::Log4perl::Level::to_priority($string);
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

bulkwhois2database  [--file FILE] [--dbms STRING] [--database STRING] 
                    [--host HOST_ADDRESS] [--port PORT_NUMBER] 
                    [--user USER_NAME] [--password PASSWORD] 
                    [--verbose STRING] [optional arguments ...]

bulkwhois2database  [--file FILE] [--dbms STRING] [--database STRING] 
                    [--host HOST_ADDRESS] [--port PORT_NUMBER] 
                    [--user USER_NAME] [--password PASSWORD] 
                    [--verbose STRING] [--log FILE]

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

=item B<-v STRING, --verbose STRING>

    Set the verbosity of the application. Use one of the four values.
        FATAL
        ERROR
        WARN
        INFO
        DEBUG
        TRACE
        ALL
        OFF
    Refer to log4Perl on CPAN for more information.

=item B<-l FILE, --log FILE>
    
    Enable logging to the specified file. Use this in conjunction with 
    --verbose, -v. Otherwise there will be an error. 

=back

=head1 DESCRIPTION
    
    bulkwhois2database takes in an ARIN BulkWhois xml file and dumps it to the 
    specified database. Beware! When the file is dumped to a database all of 
    the previous tables will be dropped and repopulated with the new data.

=cut


