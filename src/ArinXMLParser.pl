#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# This perl script reads an xml file and purges the contents into
# a temporary  MySQL database. Once the database has been created 
# the script performs a second pass through and creates a permanent
# MySQL database in 3rd normal form (or at least makes it's best 
# attempt"

use strict;
use warnings;
use Data::Dumper;
use XML::LibXML::Reader; #Read the file without using too much memory
use XML::Simple; #It may be easer to use xml simple for each child elment (asn, org, net, and poc).
use BulkWhois::Schema;
use InsertManager::InsertManager;
use Cwd;
use Scalar::Util 'blessed';

#Right now the default key that XML::Simple usees for element text is #TEXT. 
use constant {
    ELEMENT_TEXT => '#TEXT'
#    ELEMENT_ATTRIBUTE => '#ATTR'
};

#my $xmlPath = "/home/crmckay/Desktop/arin_db/arin_db_test.xml";
#my $xmlPath = "/home/crmckay/Desktop/arin_db/arin_db.xml";
my $xmlPath = "/home/crmckay/Desktop/arin_db/arin_db_ASN.xml";
#my $xmlPath = "/home/crmckay/Desktop/arin_db/arin_db_ASN.xml";
print getcwd."\n";
dumpXMLToSQLDB($xmlPath, dbms => 'mysql', database => 'BulkWhois', 
                hostAddress => 'localhost', username => 'root', 
                password => '12345', verbose => 1, debug => 1);

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Parses the specified .xml Arin dump file and places it in the 
#  user defined SQL database. For now this function will silently 
#  ignore the attributes for all of the elements since the Arin
#  dump file stores all of the inportant values in elements. The 
#  end result will be a set of tables in 3nd normal form.
#
#   @param .xml file path relative to the perl script directory.
#
#       been created. If not then an error will be thrown.
#   @param dbms => 'dbms'. The database management system to to connect to.
#       (any database that is supported by Class::DBI).
#   @param database => the database to connect to. The database must 
#       exist before running this script.
#   @param hostAddress => 'host address'. The ip of the host.
#   @param port => 'port no.'. The port to connect to.
#   @param username => 'username'.
#   @param password => 'password'.
#   @param @optional dropTables => 'boolean value' 1 or 0 to drop the database tables
#       before parsing the xml. This will default to 0. If 0 is set then the 
#       function will only update the tables.
#   @param @optional overwriteOld => 'boolean value' 1 or 0 to overwrite old entries 
#       in the database when reading in an xml file or keep a history of updated
#       entries.
#   @param @optional verbose => 'boolean value' 1 or 0 to turn on or off verbal mode.
#   @param @optional debug => 'boolean value' 1 or 0 to turn on or off debug mode.
sub dumpXMLToSQLDB {
    my $thisFunction = (caller(0))[3];
    print "Entered: $thisFunction\n";

    #Initialize variables.
    my $xmlPath     = shift;
    my %args        = @_;
    my $debug       = ($args{'debug'}) ? $args{'debug'} : 0;
    my $verbose     = ($args{'verbose'}) ? $args{'verbose'} : 0;
    my $dbms        = ($args{'dbms'}) ? $args{'dbms'} : die "You need to specify a dbms. pass in dbms => 'SQL server type' as a parameter.\n";
    my $database   = ($args{'database'}) ? $args{'database'} : die "You need to specify a database to use. pass in database => 'a database name' as a parameter.\n";
    my $hostAddress = ($args{'hostAddress'}) ? $args{'hostAddress'} : die "Please pass in the address of the database host. Use this syntax: hostaddress => 'ip address'.\n";
    my $port        = ($args{'port'}) ? $args{'port'} : do { 
        my $init = sub { 
            print "port => 'port no.' not specified. Defaulting to port => 3306\n" if ($verbose);
            return 3306;
        };
        $init->();
    };
    my $username    = ($args{'username'}) ? $args{'username'} : die "A username needs to be passed in. username => 'username' as a parameter.\n";
    my $password    = ($args{'password'}) ? $args{'password'} : do { 
        print "password => 'password' not specified. Assuming that no password is needed.\n" if ($verbose); 
    };
    print "Done getting args\n" if($debug);


    #~~~~~~~~~~~~~~~~~~~~~~
    #Connect and set up the InsertManager.
    my $dsn = "dbi:$dbms:$database:$hostAddress:$port";
    if($debug) {
        print "\n############################################\n";
        print "Connection Info: $dsn\n";
        print "User: $username\n";
        print "Password: $password\n";
        print "############################################\n\n";
    } 
    my $bulkWhoisSchema = BulkWhois::Schema->connect($dsn, $username, $password);
    #@TODO I should probably have a more intelligent deploying scheme.
    $bulkWhoisSchema->deploy({add_drop_table => 1}); #Creates a database from the schema erase previouse db
    my $deployStatements = $bulkWhoisSchema->deployment_statements;
    print "$deployStatements\n";

    my $insertManager = InsertManager::InsertManager->new(bufferSize => 1000, schema => $bulkWhoisSchema);
    $insertManager->defaultElementTextKey(ELEMENT_TEXT); 
    #~~~~~~~~~~~~~~~~~~~~~~~~

    #Make sure the file path is valid. If it is then initialize an XML::LibXML::Reader 
    # object.
    print "Checking the file's path\n" if($debug);
    my $xmlReader = (fileExists($xmlPath)) 
                    ? XML::LibXML::Reader->new(
                                    'location' => $xmlPath, 
                                    'load_ext_dtd' => 0
                                    )
                    : die $xmlPath . " is an invalid path\n";

    #Count the number of lines in the file for a progress report.
    #Set the refresh rate afterwards. This will print an update of 
    #the reading progress for 400 times throughout the dumping.
    print "Counting lines\n" if($debug);
    my ($totalLines, $deltaTime) = (10000, 0);#($verbose) ? countLinesInFile($xmlPath) : 0;
#    print Dumper $totalLines, $deltaTime;
    my $counter = 0;
    my $refreshRate = (($totalLines / 400) < 1) ? 1 : int($totalLines / 400);
    print "Time to count lines: $deltaTime seconds\n" if($debug);
    print "Lines counted: $totalLines\n" if($debug);
    print "Refresh every $refreshRate lines parsed\n" if($debug); 

    #Loop through the contents of the .xml file. Store all of the elements into the 
    #database.
    print "Begin reading\n" if($debug); 
    my $BUFFER_SIZE = 2000; #@TODO remove this variable once done debugging this script.
    my $startTime = time; #Start the stopwatch
    while($xmlReader->read()) {
        #Go through all of the child elements of the root node. Use XML::Simple
        # to convert them into a hash. Then go through the hash and push it into 
        # the database.
        if(($xmlReader->depth > 0) &&
            ($xmlReader->nodeType() != XML_READER_TYPE_SIGNIFICANT_WHITESPACE)) {

            #Print the progress to the screen.
            if(!($counter % $refreshRate) && $verbose) {
                my $percentComplete = int((($xmlReader->lineNumber()) / $totalLines) * 100);
                print "$percentComplete% of the file has been processed\n";
            }
            
            #Use XML::Simple to load each element into memory directly.
            my %parsedXML = ();
            $parsedXML{$xmlReader->name} = XMLin($xmlReader->readOuterXml(), ForceContent => 0, ForceArray => 0,  ContentKey => ELEMENT_TEXT);
            
#            print Dumper \%parsedXML;
            
            #Push the hash into the InsertManager object. 
            $insertManager->addRowToBuffer(\%parsedXML);

            $xmlReader->next();
            $counter++;
            do{ 
                #$insertManager->dumpBuffer;
                #last;
                print "Iteration: $counter\n" if($debug);
            } if(($counter % 1000) == 0);
        }#END IF
    }#END WHILE
    my $endTime = time;
    $deltaTime = $endTime - $startTime;
    print "$deltaTime seconds was required to parse the XML file\n" if($verbose || $debug);
    
    #@TODO Get the stopping time.
    # Print the total time to the screen. 
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

#### SCRAP #########
#


            #Now take that has and push it into the SQL database.   
            #Convert the multi-dimensional hash into a set of tables.
            #@TODO call the appropriate functions to initialize the DBIx::Class
            # obejects based on the XMLin hash.
            #if($counter == 0) { 
            #    my $tmpArray = [qw/asnHandle ref startAsNumber endAsNumber name registrationDate updateTime/];
            #    push(@insertBuffer, $tmpArray);
            #}
            #elsif($xmlReader->name eq "asn") {
            #    my $tmpArray = [
            #        $parsedXML{'asn'}->{'handle'},
            #        $parsedXML{'asn'}->{'ref'},
            #        $parsedXML{'asn'}->{'startAsNumber'},
            #        $parsedXML{'asn'}->{'endAsNumber'},
            #        $parsedXML{'asn'}->{'name'},
            #        $parsedXML{'asn'}->{'registrationDate'},
            #        $parsedXML{'asn'}->{'updateDate'}
            #    ];
                #{
                #    asnHandle => $parsedXML{'asn'}->{'handle'},
                #    ref => $parsedXML{'asn'}->{'ref'},
                #    startAsNumber => $parsedXML{'asn'}->{'startAsNumber'},
                #    endAsNumber => $parsedXML{'asn'}->{'endAsNumber'},
                #    name => $parsedXML{'asn'}->{'name'},
                #    registrationDate => $parsedXML{'asn'}->{'registrationDate'},
                #    updateTime => $parsedXML{'asn'}->{'updateDate'}
                #};
            #    push(@insertBuffer, $tmpArray);
            #}

            #if(@insertBuffer == $BUFFER_SIZE) {
            #    my $rowsToInsert = $bulkWhoisSchema->resultset('Asns')->populate(
            #        \@insertBuffer
            #    );
            #    print "Completed population\n";
            #}


