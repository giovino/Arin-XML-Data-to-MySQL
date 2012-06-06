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
#use XML::LibXML::AttributeHash; #Reader does not provide the ability
#                                # therefore this module is needed.
use XML::Simple; #It may be easer to use xml simple for each child elment
                            #which is not very large.
use constant {
    ELEMENT_TEXT => '#TEXT',
    ELEMENT_ATTRIBUTE => '#ATTR'
};

my $xmlPath = "arin_db_test.xml";
dumpXMLToSQLDB($xmlPath, undef, undef, undef, undef, undef, undef, 0, 1);

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
#   @param the database to write to. Assumes that is has already 
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
        #Recursivly parse the child elements of the root node.
        if(($xmlReader->depth > 0) &&
            ($xmlReader->nodeType() != XML_READER_TYPE_SIGNIFICANT_WHITESPACE)) {

            #Print the progress to the screen.
            if(!($counter % $refreshRate) && $verbal) {
                my $percentComplete = int((($xmlReader->lineNumber()) / $totalLines) * 100);
                print "$percentComplete% of the file has been processed\n";
            }

            print "Has ". $xmlReader->attributeCount() ." attributes\n" if $debug;

#            tie my %hash, 'XML::LibXML::AttributeHash', $xmlReader->readOuterXml();
#            print "Attribute Hash->".Dumper(\%hash);

            #call the recursive method to get a hash. If the 
            # contents of the element is only a string then 
            # store the string in the hash.
#           my %elementHash = ();
#            if($xmlReader->nodeType != XML_READER_TYPE_TEXT) {
#                $elementHash{$xmlReader->name()} = xmlElementToHash($xmlReader->readOuterXml(),
#                                                                        $debug, $xmlReader->depth + 1);
#            }
#            else {
#                $elementHash{ATTRIBUTE_TEXT} = $xmlReader->value;
#            }
#
#            print Dumper \%elementHash;
 
#            xmlElementToHashSimple($xmlReader->readOuterXml());
            my $parsedXML = XMLin($xmlString, ForceContent => 0, ForceArray => 0,  ContentKey => ELEMENT_TEXT)

            $xmlReader->next();
            $counter++; 
             
            #Now take that has and push it into the MySQL database.   
            #Convert the multi-dimensional hash into a set of tables.
            # @TODO hashToTables(%hashToConvert);
        }
    }
}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Reads an xml element and converts it into a multi dimensional 
#  hash. The hash keys will be the element names and their respective
#  values will be a reference to a hash or a string. 
#   
#   @param a string containing the xml to recursively parse.
#   @param @optional pass in 1 to enable debugging.
#   @param @optional the depth in the xml.
#
#   @return a scalar containing a hash reference if the element has
#       child elements. Otherwise return a scalar of the value of the
#       element.
sub xmlElementToHash {
    my $xmlString = shift;
    my $debug = shift;
    my $depth = shift;

    $debug = ($debug) ? $debug : 0; #Set debug to false if the value of $debug is 'undef'
    $depth = ($depth) ? $depth : 0; #Same for $depth.
    
    my $xmlReader = XML::LibXML::Reader->new(string => $xmlString, load_ext_dtd => 0);
    
    my %elementHash = (); #initialize an empty hash to represent the element.

    #Recursively call this function to parse the sub elements.
    while($xmlReader->read()) {
        #Parse the xml object.
        #@TODO Need to find a way to add the attributes into the hash.
       if(($xmlReader->depth > 0) &&
       ($xmlReader->nodeType() != XML_READER_TYPE_SIGNIFICANT_WHITESPACE)) { 
            
            #For debugging.
            if($debug) {
                my $d = $depth;
                print "-"x$d."Name: ".$xmlReader->name()."\n" if($xmlReader->nodeType != 3);
                print "-"x$d."Local Name: ".$xmlReader->localName()."\n";
                print "-"x$d."Node Type: ".$xmlReader->nodeType()."\n";
     	        print "-"x$d."Depth: ".$xmlReader->depth."\n";
     	        my $value = ($xmlReader->value) ? $xmlReader->value : "No Value";
                my $hasValue = $xmlReader->hasValue(); 
                my $hasAttributes = $xmlReader->attributeCount();
                print "-"x$d."Has ". $hasAttributes ." attributes\n";
                print "-"x$d."Is Empty: ". $xmlReader->isEmptyElement() ."\n";
                print "-"x$d."Has Value: ". $hasValue ."\n";
                print "-"x$d."Value: ".$value."\n";
                print "-"x$d."Inner XML ". $xmlReader->readInnerXml() ."\n";
                
#                print "READING->".Dumper $xmlReader->readAttributeValue();

                foreach(0...($hasAttributes - 1)) {
                    print "-"x$d."Attribute $_: ". $xmlReader->getAttributeNo($_) ."\n";
                }
            }
            
            #Store in the hash a key<=>hash_ref pair or key<=>string pair. 
            if($xmlReader->nodeType != XML_READER_TYPE_TEXT) {
                $elementHash{$xmlReader->name()} = xmlElementToHash($xmlReader->readOuterXml(),
                                                                        $debug, $depth + 1);
            }
            else {
                $elementHash{'#TEXT'} = $xmlReader->value;  
            }

            $xmlReader->next();
        }#END IF
    }#END WHILE

    return \%elementHash;
}

sub xmlElementToHashSimple {
    my $xmlString = shift;

    
    my $parsedXML = XMLin($xmlString, ForceContent => 0, ForceArray => 0,  ContentKey => ELEMENT_TEXT);

    return $parsedXML; #Returns the Xml::Simple generated hash. 
}

__END__

##################################  SCRAP  ############################################
#Use LibXML::Reader to read the test data.
sub libXMLReader {
    my $path = shift;

    my $xmlReader = XML::LibXML::Reader->new('location' => $path, 
        load_ext_dtd => 0);
    
    #Count the number of lines in the file for a progress report.
    my $totalLines;
    if(1) {
        $totalLines = 0;
        
        open(XML, "<$path");
        while(<XML>) {
            $totalLines++;
        }
        close(XML);
    }

    #$xmlReader->read();
    #xmlElementToHash($xmlReader->readOuterXml(), 1, 0); return;
    my $counter = 0;
    my $refreshRate = (($totalLines / 400) < 1) ? 1 : int($totalLines / 400);
    while($xmlReader->read()) {

        #First go through the child elements of the root node.
        if(($xmlReader->depth > 0) && ($xmlReader->nodeType() != 14)) {

            #Print the progress to the screen.
            if(!($counter % $refreshRate)) {
                my $percentComplete = int((($xmlReader->lineNumber()) / $totalLines) * 100);
                print "$percentComplete% of the file has been processed\n";
            }

            #call the recursive method to get a hash. If the 
            # contents of the element is only a string then 
            # store the string in the hash.
            my %elementHashRef = ();
            if($xmlReader->nodeType != 3) {
                $elementHashRef{$xmlReader->name()} = xmlElementToHash($xmlReader->readOuterXml()); #1, $d + 1);
            }
            else {
                $elementHashRef{$xmlReader->name()} = $xmlReader->value;
            }
            
            $xmlReader->next();

            $counter++;
        }
    }
}


