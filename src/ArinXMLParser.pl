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
use XML::Simple; #It may be easer to use xml simple for each child elment
                            #which is not very large.
use constant {
    ELEMENT_TEXT => '#TEXT',
    ELEMENT_ATTRIBUTE => '#ATTR'
};

my $xmlPath = "arin_db_test.xm";
dumpXMLToSQLDB($xmlPath, dbms => 'mysql', hostaddress => '127.0.0.1', username => 'root', password => '12345', verbose => 1, debug => 1);

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
#   @param dbms => 'dbms'. The database to connect to. (any database that 
#       is supported by Class::DBI).
#   @param hostaddress => 'host address'. The ip of the host.
#   @param port => 'port no.'. The port to connect to.
#   @param username => 'username'.
#   @param password => 'password'.
#   @param @optional dropdatabase => 'boolean value' 1 or 0 to drop the database
#       before parsing the xml. This will default to 0. If 0 is set then the 
#       function will only update the database. Otherwise it will create a new 
#       database.
#   @param @optional overwrite => 'boolean value' 1 or 0 to overwrite old entries 
#       in the database when reading in an xml file or keep a history of updated
#       entries.
#   @param @optional verbose => 'boolean value' 1 or 0 to turn on or off verbal mode.
#   @param @optional debug => 'boolean value' 1 or 0 to turn on or off debug mode.
sub dumpXMLToSQLDB {
    my $xmlPath     = shift;

    #Initialize variables.
    my %args        = @_;
    my $debug       = ($args{'debug'}) ? $args{'debug'} : 0;
    my $verbose     = ($args{'verbose'}) ? $args{'verbose'} : 0;
    my $dbms        = ($args{'dbms'}) ? $args{'dbms'} : die "You need to specify a dbms. pass in dbms => 'a database name' as a parameter.\n";
    my $hostaddress = ($args{'hostaddress'}) ? $args{'hostaddress'} : die "Please pass in the address of the database host. Use this syntax: hostaddress => 'ip address'.\n";
    my $port        = ($args{'port'}) ? $args{'port'} : do { my $init = sub { 
            print "port => 'port no.' not specified. Defaulting to port => 3306\n" if ($verbose);
            return 3306;
        };
        $init->();
    };
    my $username    = ($args{'username'}) ? $args{'username'} : die "A username needs to be passed in. username => 'username' as a parameter.\n";
    my $password    = ($args{'password'}) ? $args{'password'} : print "password => 'password' not specified. Assuming that no password is needed.\n" if ($verbose);

    #Make sure the file path is valid. If it is then initialize an XML::LibXML::Reader 
    # object.
    my $xmlReader = undef;
    if(-e $xmlPath) {
        $xmlReader = XML::LibXML::Reader->new('location' => $xmlPath, 
        'load_ext_dtd' => 0); #load the xml file.
    }
    else {
        die "Invalid xml file path.\n";
    }
   
    #Count the number of lines in the file for a progress report.
    #Set the refresh rate afterwards. This will print an update of 
    #the reading progress for 400 times throughout the dumping.
    my $totalLines = 0;
    if($verbose) {
        $totalLines = 0;
        
        open(XML, "<$xmlPath");
        while(<XML>) {
            $totalLines++;
        }
        close(XML);
    }
    my $counter = 0;
    my $refreshRate = (($totalLines / 400) < 1) ? 1 : int($totalLines / 400);
   
    #@TODO Get the starting time.
    
    #Store all of the children elements of the root element as table items in 
    #the database. There will be a table for each different type of child element.
    #(e.g. There will be at least an 'asn', 'poc', 'org' and 'net' tables.
    while($xmlReader->read()) {
        #Go through all of the child elements of the root node. Use XML::Simple
        # to convert them into a hash.
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

            $xmlReader->next();
            $counter++; 
             
            #Now take that has and push it into the MySQL database.   
            #Convert the multi-dimensional hash into a set of tables.
            # @TODO hashToTables(%hashToConvert);
        }
    }
    
    #@TODO Get the stopping time.
    # Print the total time to the screen.
    
}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# This function takes in a hash generated by the function XMLin() 
# from the XML::Simple module. The function expects that when calling
# XMLin ForceContent and ForceArray have been set to zero. 
#
#   @param 
#
sub simpleHashToSql {
    my $hash = shift;
}

__END__
