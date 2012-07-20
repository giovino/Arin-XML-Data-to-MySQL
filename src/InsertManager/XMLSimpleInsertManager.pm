#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# The XMLSimpleInsertManager class takes in an xml hash related
# to the whois database and performs an insert to the proper
# table. This elimates all the nasty details. A nice feature
# to InsertManager is that it will buffer up all the inserts
# and then perform a bulk insert.
#
# @dependency InsertManager::InsertManagerInterface
# @dependency InsertManager::Mappings
#
package InsertManager::XMLSimpleInsertManager;
use base qw/InsertManager::InsertManagerInterface/; #Implement the interface
use InsertManager::Mappings;
use InsertManager::BufferManager;

use strict;
use warnings;
use Data::Dumper;
use BulkWhois::Schema;
use Scalar::Util 'blessed';
use JSON;
use Switch;
use XML::Simple; #It may be easer to use xml simple for each child elment (asn, org, net, and poc).
$XML::Simple::PREFERRED_PARSER = 'XML::LibXML::SAX'; #Makes XML::Simple Run faster

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Create a new InsertManager. 
#   
#   @param bufferSize => 'the buffer size'. The maximum size 
#       of the buffer before a bulk insert is performed.
#   @param schema => $schemaObject. A refernce to the
#       DBIx::Class::Schema object. This will be used 
#       to perform the inserts on.
#   @param logger => log4Perl logger object. 
#
sub new {
    #Get the argumensts
    my $class = shift;
    my %args = @_;
    my $bufferSize  = ($args{'bufferSize'}) ? $args{'bufferSize'}   : 4095;
    my $verbosity   = ($args{'verbosity'})  ? $args{'verbosity'}    : 0;
    #Make sure a schema object is passed in. Otherwise die.
    my $schemaObj   = (blessed($args{'schema'}) && (blessed($args{'schema'}) eq "BulkWhois::Schema")) 
                    ? $args{'schema'} 
                    : die "I need a schema object, Create one and pass it in.\n"; 
    my $logger   = (blessed($args{'logger'}) && (blessed($args{'logger'}) eq "Log::Log4perl::Logger")) 
                    ? $args{'logger'} 
                    : die "I need a log4perl object, Create one and pass it in.\n";
    my $self->{ITEMS_PROCESSED} = 0;
    $self->{DEFAULT_ELEMENT_TEXT_KEY} = undef;

    my $buffer = InsertManager::BufferManager->new(bufferSize => $bufferSize, schema => $schemaObj, logger => $logger);
    $self->{BUFFER} = $buffer;  #Stores the buffer object
    $self->{LOGGER} = $logger;    #Stores a logger object.

    #Perform the blessing
    bless $self, $class;
    
    $self->log->trace("XMLSimpleInsertManger Initialized");

    return $self;
}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# This method is overriding InsertManager::InsertManagerInterface::parseXML.
# All it does is it takes in a sub element and uses XML::Simple::XMLin to 
# convert it into a hash. It then passes that hash to addRowToBuffer for 
# processing.
#
# @param xml
#
sub parseXML {
    my $self = shift;
    my $xml = shift;
    my $xmlElementName = shift;

    if(!defined $self->{DEFAULT_ELEMENT_TEXT_KEY}) {
        die "Please set the ContentKey for XMLin by calling the method defaultElementTextKey and setting a value\n";
    }
    
    #Use XML::Simple to load each element into memory directly.
    my %parsedXML = ();
    $parsedXML{$xmlElementName} = XMLin($xml, ForceContent => 0, ForceArray => 0,  ContentKey => $self->{DEFAULT_ELEMENT_TEXT_KEY});
            
    #Push the hash into the InsertManager object. 
    $self->addRowToBuffer(\%parsedXML);
}#END parseXML

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Overrides InsertManager::InsertManagerInterface::endParsing. Flushes any 
# remaining items to the database.
sub endParsing {
    my $self = shift;

    $self->{BUFFER}->insertAndFlushBuffer;
}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Adds a new row to the buffer. Takes in a hash, parses it to
# the correct format and adds it to the buffer to bulk insert
# into the database.
sub addRowToBuffer {
    my $self = shift;
    my $rowsToPush = shift;

    #Determine the type of table it will need to be parsed to.
    while (my ($key, $value) = each(%{$rowsToPush})) {
        #Determine wich table the hash goes to. Ignore the case
        #of $key
        if($key =~ m/asn/i) {
            
            $self->{BUFFER}->pushToBuffer('Asns', $self->simpleHashForRowHash($value, 'Asns', 'asn'));
        }
        elsif($key =~ m/poc/i) { 
            $self->{BUFFER}->pushToBuffer('Pocs', $self->simpleHashForRowHash($value, 'Pocs', 'poc'));
        }
        elsif($key =~ m/org/i) { 
            $self->{BUFFER}->pushToBuffer('Orgs', $self->simpleHashForRowHash($value, 'Orgs', 'org'));
        }
        elsif($key =~ m/net/i) { 
            $self->{BUFFER}->pushToBuffer('Nets', $self->simpleHashForRowHash($value, 'Nets', 'net'));
        }
        else {
            $self->insertAndFlushBuffer;
            $self->log->error((Dumper $key, $value));
            $self->log->error("Unable to find a function that can handle the hash you passed in.");
            $self->log->error("Items Processed: ", $self->{ITEMS_PROCESSED});
            if($self->log->is_error()) {
                $self->log->error("Application exiting at ", (caller(0))[3]);
                print "Application died. Check log\n";
                exit;
            }
        }

        $self->{ITEMS_PROCESSED}++;
    }
}#END addRowToBuffer

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Takes in a hash from XML::Simple::XMLin and converts it 
# into a hash that can used by DBIx::Class::Schema::populate.
#
#   @dependency Mappings.pm
#
#   @param a row to push into the database. The expected variable type
#       is a hash that has been produced by XML::Simple::XMLin(...)
#   @param the corresponding table to parse the return hash to.
#   @param the name of the element being parsed.
#
#   @return a hash that can be used by the populate method in DBIx::Class::Schema object.
sub simpleHashForRowHash {
    my $self = shift;
    my $rowToPush = shift;
    my $table = shift;
    my $element = shift;
    
    #TODO Make sure that there are no elements in xml that are not defined in the mappings.

    my %tmpHash = ();
    #parse all of the simple elements in the hash.
    foreach(@{$TABLES->{$table}}) {
        my $corresXMLElement = ${$COLUMN_TO_XML_MAPPINGS->{$table}}{$_}; #Get the column that corresponds to the xml element.

        if(!$ELEMENTS_THAT_NEED_EXTRA_PARSING->{$element}->{$corresXMLElement}) {
            $tmpHash{$_} = $rowToPush->{$corresXMLElement};
        }
    }

    #Go through all of the elements that need extra parsing
    foreach my $key (keys $ELEMENTS_THAT_NEED_EXTRA_PARSING->{$element}) {
        my $column = $XML_TO_COLUMN_MAPPINGS->{$table}->{$key};

        #If there is a mapping then call the parsingFunctionChooser and 
        #push the result into the tmpHash.
        if(defined($column)) {    
            $tmpHash{$column} = $self->parsingFunctionChooser($rowToPush, $element, $key);
        }
        #If column is null then call the parsingFunctionChooser and assume that the 
        # function parsingFunctionChooser calls will insert the data into another
        # table. 
        else {
            $self->parsingFunctionChooser($rowToPush, $element, $key);
        }
    }
        
    return \%tmpHash;
}#END simpleHashForRowHash

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Converts a comment into a string so that it may be added 
# to the database.
#
#   @param The comment hash to parse.
#
#   @return The comment as a single string.
sub commentToString {
    my $self = shift;
    my $commentToParse = shift;

    my $comment = undef;

    #First see if dealing with a hash or an array of hashes.
    #Then parse accordingly.
    if(ref($commentToParse->{'line'}) eq 'ARRAY') {
        my $count = @{$commentToParse->{'line'}}; 
        my @comments = ('')x$count;

        #Map the comment contents of the array to their proper location in the comment only array.
        map { 
                $comments[$_->{'number'}] = ($_->{$self->defaultElementTextKey}) 
                                            ? $_->{$self->defaultElementTextKey}
                                            : ''
            }
            @{$commentToParse->{'line'}};
        $comment = join " ", @comments;
    }
    elsif(ref($commentToParse->{'line'}) eq 'HASH') {
        $comment = $commentToParse->{'line'}->{$self->defaultElementTextKey};
    }
    elsif(!defined($commentToParse->{line})) {}
    else {
        $self->log->error("Unexpected value when received a comment to parse.");
        $self->log->error(Dumper $commentToParse); 
    }

    return $comment;
}#END commentToString

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Add a row to a *_Emails table
#
#   @param the emails to add.
#   @param the pocHandle they belong to
sub addEmails {
    my $self = shift;
    my $tableToUpdate = shift;
    my $parentHandle = shift;
    my $emails = shift;

    my @table = @{$TABLES->{$tableToUpdate}};
    my %colMapps = %{$COLUMN_TO_XML_MAPPINGS->{$tableToUpdate}};
    if(!defined($emails) || !defined($emails->{'email'})) {
        return;
    }
    elsif(ref($emails->{'email'}) =~ m/ARRAY/) {
        foreach(@{$emails->{'email'}}) { 
            if(ref($_) =~ m/HASH/) {#Just incase there is extra data in the emails element
                $self->log->error("Unhandled xml simple hash structure.");
                $self->log->error("I was expecting an array of scalars but I got this.");
                $self->log->error('$_: ', (Dumper $_));
                $self->log->error("tableToUpdate: $tableToUpdate");
                $self->log->error("parentHandle: $parentHandle\n");
                $self->log->error("emails: ".Dumper $emails);
                print "There has been an error while parsing the data. Please check the log file\n";
                die;
            }
            my %tmpHash = ();
            $tmpHash{$TABLES->{$tableToUpdate}->[0]} = $parentHandle; #Always assumes that column 0 stores the parentHandle in the database.
            $tmpHash{$TABLES->{$tableToUpdate}->[1]} = $_; #Same for column 1 but for email
            $self->{BUFFER}->pushToBuffer($tableToUpdate, \%tmpHash);
        }
             
        return;
    }
    elsif(ref($emails->{'email'}) eq '') {
        my $tmpHash = {
            $TABLES->{$tableToUpdate}->[0] => $parentHandle, 
            $TABLES->{$tableToUpdate}->[1] => $emails->{'email'}
        };
        $self->{BUFFER}->pushToBuffer($tableToUpdate, $tmpHash);
        return;
    }
    else {
        $self->log->error("Unexpected value when received an email element to parse.\n");
        $self->log->error((Dumper $emails)); 
        print "Application died. Check log\n";
        exit;
    }
}#END addEmails

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Adds a set of phone numbers to a table and binds them 
# to a handle. The function will also store the phone
# type if available.
#
#   @dependency Mappings.pm
#
#   @param the table to update. (this table needs to be defined in Mappings.pm)
#   @param the handle of the element the phones element was
#       found in.
#   @param the phones to add.
sub addPhones {
    my $self = shift;
    my $tableToUpdate = shift;
    my $parentHandle = shift;
    my $phones = shift;

    if(!defined($phones) || !defined($phones->{'phone'})) {
        return;
    }
    elsif(ref($phones->{'phone'}) =~ m/ARRAY/) {
        my @hashArray = ();
        foreach(@{$phones->{'phone'}}) {  
            my $handle = $_->{'number'}->{'pocHandle'};
            if($handle ne $parentHandle) {
                $self->log->error("The parent handle $parentHandle doesn't match");
                $self->log->error("the handle $handle in the phone element");
                print "Application died. Check log\n";
                exit;
            }

            my $tmpHash = {
                $TABLES->{$tableToUpdate}->[0] => $parentHandle, 
                $TABLES->{$tableToUpdate}->[1] => $_->{'number'}->{'phoneNumber'},
                $TABLES->{$tableToUpdate}->[2] => $_->{'type'}->{'description'}
            };
            push @hashArray, $tmpHash;
        }
        $self->{BUFFER}->pushToBuffer($tableToUpdate, @hashArray);
        return;
    }
    elsif(ref($phones->{'phone'}) =~ m/HASH/) { 
        my $handle = $phones->{'phone'}->{'number'}->{'pocHandle'};
        if($handle ne $parentHandle) {
            $self->log->error("The parent handle $parentHandle doesn't match");
            $self->log->error("the handle $handle in the phone element");
            print "Application died. Check log\n";
            exit;
        }
        
        my $tmpHash = {
            $TABLES->{$tableToUpdate}->[0] => $parentHandle, 
            $TABLES->{$tableToUpdate}->[1] => $phones->{'phone'}->{'number'}->{'phoneNumber'},
            $TABLES->{$tableToUpdate}->[2] => $phones->{'phone'}->{'type'}->{'description'}
        };
        $self->{BUFFER}->pushToBuffer($tableToUpdate, $tmpHash);
        return;
    }
    else { 
        $self->log->error("Unexpected value when received an email element to parse.\n");
        $self->log->error(Dumper $phones); 
        print "Application died. Check log\n";
        exit;
    }
}#END addPhones

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Add a pocLink reference row to the proper binding table. 
# this function assumes that you have created the table before
# hand.
# 
#   @param the table to parse the pocLinks to.
#   @param the column name of the handle that the pocLinks 
#       belong to. (e.g. pass in asnHandle if you found the 
#       pocLinks element in the asn element)
#   @param the pocsLink element that is a hash (from XMLin).
sub addPocLinks {
    my $self = shift;
    my $tableToUpdate = shift;
    my $parentHandle = shift;
    my $pocLinks = shift;
    
    #Begin parsing the poc links.
    #First see if dealing with a hash or an array of hashes.
    #Then parse accordingly.
    my @table = @{$TABLES->{$tableToUpdate}};
    my %colMapps = %{$COLUMN_TO_XML_MAPPINGS->{$tableToUpdate}};
    if(ref($pocLinks->{'pocLink'}) eq 'ARRAY') {
        foreach(@{$pocLinks->{'pocLink'}}) {
            my %tmpHash = ();
            $tmpHash{$table[0]} = $parentHandle; 
            $tmpHash{$table[1]} = 
                $_->{$colMapps{$table[1]}}; #get value of handle
            $tmpHash{$table[2]} = 
                $_->{$colMapps{$table[2]}}; #get value of function
            $tmpHash{$table[3]} = 
                $_->{$colMapps{$table[3]}}; #Get value of description

            $self->{BUFFER}->pushToBuffer($tableToUpdate, \%tmpHash);
        }
        return 1;
    }
    elsif(ref($pocLinks->{'pocLink'}) eq 'HASH') {
        my $pocLink = $pocLinks->{'pocLink'};

        my %tmpHash = ();
        $tmpHash{$table[0]} = $parentHandle; 
        $tmpHash{$table[1]} = 
            $pocLink->{$colMapps{$table[1]}}; #get value of handle
        $tmpHash{$table[2]} = 
            $pocLink->{$colMapps{$table[2]}}; #get value of function
        $tmpHash{$table[3]} = 
            $pocLink->{$colMapps{$table[3]}}; #Get value of description
        
        $self->{BUFFER}->pushToBuffer($tableToUpdate, \%tmpHash);
        return 1;
    }
    elsif(!defined($pocLinks->{'pocLink'})) {return 1;}
    else {
        $self->log->error("Unexpected value when received a pocLinks element to parse.");
        $self->log->error((Dumper $pocLinks));  
        return 0;
    }
}#END addPocLinks

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# addNetBlock takes a netBlocks XMLin element to hash and 
# formats it such that it can be passed into the method
# simpleHashForRowHash. The outputted value is then pushed 
# into the buffer
#
#   @param the table to store the netblock in.
#   @param the handle the netblock belongs to.
#   @param the set of netblocks to process.
sub addNetBlock {    
    my $self = shift; 
    my $tableToUpdate = shift;
    my $parentHandle = shift;
    my $netBlocks = shift;

    if(!defined($netBlocks->{'netBlock'})) {
        $self->log->warn("There are no netblocks assigned to this net");
    }
    elsif(ref($netBlocks->{'netBlock'}) =~ m/HASH/) {
        $netBlocks->{'netBlock'}->{$TABLES->{$tableToUpdate}->[0]} = $parentHandle; #assume the first col will store the parent handle 
        $self->{BUFFER}->pushToBuffer($tableToUpdate, $self->simpleHashForRowHash($netBlocks->{'netBlock'}, $tableToUpdate, 'netBlock'));
    }
    elsif(ref($netBlocks->{'netBlock'}) =~ m/ARRAY/) {
        my @hashArray = ();
        foreach(@{$netBlocks->{'netBlock'}}) {
            $_->{$TABLES->{$tableToUpdate}->[0]} = $parentHandle; #assume the first col will store the parent handle 
            push @hashArray, $self->simpleHashForRowHash($_, $tableToUpdate, 'netBlock');
        }
        $self->{BUFFER}->pushToBuffer($tableToUpdate, @hashArray); #TODO later collapse into a single push
    }
    else {
        $self->log->error("addNetBlock has encountered an unexpected value.");
        $self->log->error("Dumping to screen:");
        $self->log->error("Table to update: $tableToUpdate");
        $self->log->error("Parent handle: $parentHandle");
        $self->log->error((Dumper $netBlocks));
    }    
}#END addNetBlock

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# addNetBlock takes a netBlocks XMLin element to hash and 
# formats it such that it can be passed into the method
# simpleHashForRowHash. The outputted value is then pushed 
# into the buffer
#
#   @param the table to store the OriginASes in.
#   @param the handle the OriginASes belong to.
#   @param the set of OriginASes to process.
sub addOriginASes {    
    my $self = shift; 
    my $tableToUpdate = shift;
    my $parentHandle = shift;
    my $originASes = shift;
    
    if(!defined($originASes->{'originAS'})) {}
    elsif(ref($originASes->{'originAS'}) eq '') {
        my %tmpHash = ();
        $tmpHash{$TABLES->{$tableToUpdate}->[0]} = $parentHandle;
        $tmpHash{$TABLES->{$tableToUpdate}->[1]} = $originASes->{'originAS'};

        $self->{BUFFER}->pushToBuffer($tableToUpdate, \%tmpHash);
    }
    elsif(ref($originASes->{'originAS'}) =~ m/ARRAY/) {
        foreach(@{$originASes->{'originAS'}}) {    
            my %tmpHash = ();
            $tmpHash{$TABLES->{$tableToUpdate}->[0]} = $parentHandle;
            $tmpHash{$TABLES->{$tableToUpdate}->[1]} = $_;

            $self->{BUFFER}->pushToBuffer($tableToUpdate, \%tmpHash);
        } 
    }
    else {
        $self->log->error("addOriginAS has encountered an unexpected value.");
        $self->log->error("Dumping to screen:");
        $self->log->error("Table to update: $tableToUpdate");
        $self->log->error("Parent handle: $parentHandle");
        $self->log->error("Type: ". ref($originASes->{'originAS'}) ."\n");
        $self->log->error(Dumper $originASes);
    }   
}#END OriginASes

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Converts an address to a string so that it may be added 
# to the database table.
#
#   @param The address hash to parse.
#
#   @return The address as a single string.
sub addressToString {
    my $self = shift;
    my $addressToParse = shift;

    my $address = undef;

    #First see if dealing with a hash or an array of hashes.
    #Then parse accordingly.
    if(ref($addressToParse->{'line'}) eq 'ARRAY') {
        my $count = @{$addressToParse->{'line'}}; 
        my @addressLines = ('')x$count;

        #Map the comment contents of the array to their proper location in the comment only array.
        map { 
                $addressLines[$_->{'number'}] = ($_->{$self->defaultElementTextKey}) 
                                            ? $_->{$self->defaultElementTextKey}
                                            : ''
            }
            @{$addressToParse->{'line'}};
        $address = join "\n", @addressLines;
    }
    elsif(ref($addressToParse->{'line'}) eq 'HASH') {
        $address = $addressToParse->{'line'}->{$self->defaultElementTextKey};
    }
    elsif(!defined($addressToParse->{line})) {}
    else {
        $self->log->error("Unexpected value when received an address to parse.\n");
        $self->log->error((Dumper $addressToParse)); 
    }

    return $address;
}#END addressToString

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# parsingFunctionChooser pretty much serves as a front 
# controller. If there is an xml element that needs extra 
# parsing define a new function and add the exception here.
# 
#   @param the rowToPush (expects the same hash as the function
#       that called parsingFunctionChooser)
#   @param the element to parse in the rowToPush hash.
#   @param the sub element to parse in the hash.
#
#   @return the parsed value as a string.
sub parsingFunctionChooser {
    my $self = shift;
    my $rowToPush = shift;
    my $elementToParse = shift;
    my $subElementToParse = shift;

    my $result = undef;
    switch ($elementToParse) {
        case 'asn' {
            switch ($subElementToParse) {
                case 'comment' {        
                    $result = $self->commentToString($rowToPush->{$subElementToParse});
                }
                case 'pocLinks' {
                    $result = $self->addPocLinks('Asns_Pocs', $rowToPush->{'handle'}, 
                                            $rowToPush->{$subElementToParse}
                                            );
                }
                else {
                    $self->log->error("Unexpected element $subElementToParse for $elementToParse\n");
                    
                }
            }
        }
        case 'poc' {
            switch ($subElementToParse) {
                case 'streetAddress' { 
                    $result = $self->addressToString($rowToPush->{$subElementToParse});
                }
                case 'isRoleAccount' {
                    my $isRoleAccount = $rowToPush->{$subElementToParse};
                    $result = ($isRoleAccount =~ m/Y/) ? 1 : 0;
                }
                case 'iso3166-1' {
                    my $iso3166_1 = $rowToPush->{$subElementToParse};
                    $result = encode_json $iso3166_1;
                }
                case 'emails' {
                    $self->addEmails('Pocs_Emails', $rowToPush->{'handle'}, 
                                    $rowToPush->{$subElementToParse});
                }
                case 'phones' {
                    $self->addPhones('Pocs_Phones', $rowToPush->{'handle'},
                                    $rowToPush->{$subElementToParse});
                }
                else {
                    $self->log->error("Unexpected element $subElementToParse for $elementToParse\n"); 
                }
            }
        }
        case 'org' {
            switch ($subElementToParse) {
                case 'streetAddress' {
                    $result = $self->addressToString($rowToPush->{$subElementToParse});
                }
                case 'customer' { 
                    my $customer = $rowToPush->{$subElementToParse};
                    $result = ($customer =~ m/Y/) ? 1 : 0;
                }
                case 'iso3166-1' {
                    my $iso3166_1 = $rowToPush->{$subElementToParse};
                    $result = encode_json $iso3166_1;
                }
                case 'pocLinks' {
                    $result = $self->addPocLinks('Orgs_Pocs', $rowToPush->{'handle'}, 
                                            $rowToPush->{$subElementToParse}
                                            );
                }
                else {
                    $self->log->error("Unexpected element $subElementToParse for $elementToParse\n"); 
                }    
            }
        }
        case 'net' {
            switch ($subElementToParse) {
                case 'comment' { 
                    $result = $self->commentToString($rowToPush->{$subElementToParse});    
                }
                case 'originASes' { 
                   $self->addOriginASes('OriginASes', $rowToPush->{'handle'}, 
                                        $rowToPush->{'originASes'});
                }
                case 'pocLinks' { 
                    $result = $self->addPocLinks('Nets_Pocs', $rowToPush->{'handle'}, 
                                            $rowToPush->{$subElementToParse}
                                            );
                }
                case 'netBlocks' {
                    $self->addNetBlock('NetBlocks', $rowToPush->{'handle'},
                                        $rowToPush->{'netBlocks'}
                                    );    
                }
                else {
                    $self->log->error("Unexpected element $subElementToParse for $elementToParse\n"); 
                }    
            }
        }
        else {
            $self->log->error("parsingFunctionChooser: Unable to find a function to parse $subElementToParse that belongs to $elementToParse");
            print "There was an unexpected element. Application dying. Check log\n";
            exit;
        }
    }

    return $result;
}#END parsingFunctionChooser

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Set or get the default key to look for when parsing element text
# from an XML::Simple hash. 
sub defaultElementTextKey {
    my $self = shift;
    my $key = shift;
    $self->{DEFAULT_ELEMENT_TEXT_KEY} = ($key) ? $key : return $self->{DEFAULT_ELEMENT_TEXT_KEY};
}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Get the logger
sub log {
    my $self= shift;

    return $self->{LOGGER};
}

return 1;




