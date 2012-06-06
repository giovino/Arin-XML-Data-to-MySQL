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

my $xmlPath = "arin_db_test.xml";
dumpXMLToSQLDB($xmlPath, undef, undef, undef, undef, undef, undef, 1, 1);

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Parses the specified .xml Arin dump file and places it in the 
#  user defined SQL database. For now this function will silently 
#  ignore the attributes for all of the elements since the Arin
#  dump file stores all of the inportant values in elements. The 
#  end result will be a set of tables in 3nd normal form.
#
#   @todo add future support for attributes when nessesary.
#
#   @param .xml file path relative to the perl script directory.

#       been created. If not then an error will be thrown.
#   @param the username.
#   @param the password.
#   @param the address of the host.
#   @param the port to access on the host.
#   @param @optional pass in 1 to turn on verbal mode.
#   @param @optional pass in 1 to turn on debugging.
sub dumpXMLToSQLDB {
    my $xmlPath     = shift;
    my $platform    = shift;
    my $database    = shift;
    my $username    = shift;
    my $password    = shift;
    my $host        = shift;
    my $port        = shift;
    my $verbal      = (shift) ? 1 : 0;
    my $debug       = (shift) ? 1 : 0;

    
    #Attempting to use a feature called slurp. The goal is to load the file into memory for 
    # faster processing.
#    my $xmlData = "";
#    {
#        local( $/, *FH ) ;
#        open(FH, $xmlPath ) or die "Failed to open the file.\n";
#        $xmlData = <FH>;
#    }

    #@TODO Remember to tell the user if the file path is invalid.
    my $xmlReader = XML::LibXML::Reader->new('location' => $xmlPath, 
        'load_ext_dtd' => 0); #load the xml file.
   
    #Count the number of lines in the file for a progress report.
    my $totalLines = 0;
    if($verbal) {
        $totalLines = 0;
        
        open(XML, "<$xmlPath");
        while(<XML>) {
            $totalLines++;
        }
        close(XML);
    }

    my $counter = 0;
    my $refreshRate = (($totalLines / 400) < 1) ? 1 : int($totalLines / 400);
   
   
    #Go through all the non root elements and convert them into tables.
    # Once done looping there should be a database full of temp tables
    # in 2nd normal form. 
    while($xmlReader->read()) {
        #Recursivly parse the child elements of the root node. At the same 
        #time ignore white spaces.
        if(($xmlReader->depth > 0) &&
            ($xmlReader->nodeType() != XML_READER_TYPE_SIGNIFICANT_WHITESPACE)) {

            #Print the progress to the screen.
            if(!($counter % $refreshRate) && $verbal) {
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
}

__END__
