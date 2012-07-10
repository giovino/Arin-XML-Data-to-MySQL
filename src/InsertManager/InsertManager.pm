#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# The InsertManager class takes in an xml hash related
# to the whois database and performs an insert to the proper
# table. This elimates all the nasty details. A nice feature
# to InsertManager is that it will buffer up all the inserts
# and then perform a bulk insert.
#
# @dependency InsertManager::Mappings
#
package InsertManager::InsertManager;

use strict;
use warnings;
use Data::Dumper;
use BulkWhois::Schema;
use InsertManager::Mappings;    #qw($TABLES $ELEMENTS_THAT_NEED_EXTRA_PARSING 
                                #   $COLUMN_TO_XML_MAPPINGS $XML_TO_COLUMN_MAPPINGS);
use Scalar::Util 'blessed';
use JSON;
use Switch;

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Create a new InsertManager. 
#   
#   @param bufferSize => 'the buffer size'. The maximum size 
#       of the buffer before a bulk insert is performed.
#   @param schema => $schemaObject. A refernce to the
#       DBIx::Class::Schema object. This will be used 
#       to perform the inserts on.
#   @TODO @param overwrite => 1 || 0. Pass in 1 to overwite
#       if the handle matches. Otherwise add new row if
#       the update dates do not match.
sub new {
    #Get the argumensts
    my $class = shift;
    my %args = @_;
    my $bufferSize  = ($args{'bufferSize'}) ? $args{'bufferSize'} : 10;
    #Make sure a schema object is passed in. Otherwise die.
    my $schemaObj   = (blessed($args{'schema'}) && (blessed($args{'schema'}) eq "BulkWhois::Schema")) 
                    ? $args{'schema'} 
                    : die "I need a schema object, Create one and pass it in.\n";
    my $self->{BUFFER_SIZE} = $bufferSize; 
    $self->{SCHEMA_OBJECT} = $schemaObj; 
    $self->{ITEMS_PROCESSED} = 0;
    $self->{BUFFER} = {};
    $self->{DEFAULT_ELEMENT_TEXT_KEY} = {};

    #Initialize the buffer with all of the sub buffers for the tables.
    #The format will be key => array pairs.
#    print Dumper $TABLES;
    while( my ($key, $columns) = each($TABLES))  {
        $self->{BUFFER}->{$key} = [];
    }
#    print Dumper $self->{BUFFER};


    #Perform the blessing
    bless $self, $class;

    return $self;
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
            push @{$self->{BUFFER}->{'Asns'}}, $self->simpleHashForRowHash($value, 'Asns', 'asn');
            if(@{$self->{BUFFER}->{'Asns'}} == $self->{BUFFER_SIZE}) {
                $self->insertAndFlushBuffer('Asns');
                $self->insertAndFlushBuffer('Asns_Pocs');
            }
        }
        elsif($key =~ m/poc/i) { 
            push @{$self->{BUFFER}->{'Pocs'}}, $self->simpleHashForRowHash($value, 'Pocs', 'poc');
            if(@{$self->{BUFFER}->{'Pocs'}} == $self->{BUFFER_SIZE}) {
                $self->insertAndFlushBuffer('Pocs');
                $self->insertAndFlushBuffer('Pocs_Emails');
                }
        }
        else {
            $self->insertAndFlushBuffer;
            print Dumper $key, $value;
            print "Unable to find a function that can handle the hash you passed in.\n";
            print "Items Processed: ". $self->{ITEMS_PROCESSED} ,"\n";
            exit;
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

#        print "Key: ". Dumper $key;
#        print "Column: ". Dumper $column;
#        print "Row: " . Dumper $rowToPush;
#        print "Table: " . Dumper $table;
#        print "Mappings: ". Dumper $XML_TO_COLUMN_MAPPINGS->{$table};

        #If there is a mapping then call the parsingFunctionChooser and 
        #push the result into the tmpHash.
        if(defined($column)) {    
            $tmpHash{$column} = $self->parsingFunctionChooser($rowToPush, $element, $key);
#            print "Value added to $table\n";
#            print "----------------------------\n";
        }
        #If column is null then call the parsingFunctionChooser and assume that the 
        # function parsingFunctionChooser calls will insert the data into another
        # table. 
        else {
            $self->parsingFunctionChooser($rowToPush, $element, $key);
#            print "Value added to another table\n";
#            print "----------------------------\n";
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
        print "Unexpected value when received a comment to parse.\n";
        print Dumper $commentToParse; 
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

#    print "\n\nTable To Update: $tableToUpdate\n";
#    print "Parent Handle: $parentHandle\n";
#    print "Emails: ". Dumper $emails;
#    print "Is of type: ". ref($emails->{'email'})."\n";

    my @table = @{$TABLES->{$tableToUpdate}};
    my %colMapps = %{$COLUMN_TO_XML_MAPPINGS->{$tableToUpdate}};
    if(!defined($emails) || !defined($emails->{'email'})) {
#        print "No email found for this element\n";
        return;
    }
    elsif(ref($emails->{'email'}) =~ m/ARRAY/) {
        foreach(@{$emails->{'email'}}) { 
            if(ref($_) =~ m/HASH/) {#Just incase there is extra data in the emails element
                print "Unhandled xml simple hash structure.\n";
                print "I was expecting an array of scalars but I got this.\n";
                print '$_: '.Dumper $_;
                print "tableToUpdate: $tableToUpdate\n";
                print "parentHandle: $parentHandle\n";
                print "emails: ".Dumper $emails;
                exit;
            }

            my %tmpHash = ();
            $tmpHash{$TABLES->{$tableToUpdate}->[0]} = $parentHandle; #Always assumes that column 0 stores the parentHandle in the database.
            $tmpHash{$TABLES->{$tableToUpdate}->[1]} = $_; #Same for column 1 but for email
            push @{$self->{BUFFER}->{$tableToUpdate}}, \%tmpHash;
        }
             
        return;
    }
    elsif(ref($emails->{'email'}) eq '') {
        my $tmpHash = {
            $TABLES->{$tableToUpdate}->[0] => $parentHandle, 
            $TABLES->{$tableToUpdate}->[1] => $emails->{'email'}
        };
        push @{$self->{BUFFER}->{$tableToUpdate}}, $tmpHash;
        return;
    }
    else {
        print "Unexpected value when received an email element to parse.\n";
        print Dumper $emails; 
        exit;
    }
}#END addEmails

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Adds a set of phone numbers to a table and binds them 
# to a handle. The function will also store the phone
# type if available.
#
#   @param the table to update.
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
        #my @hashArray = ();
        #print Dumper $phones->{'phone'};
        foreach(@{$phones->{'phone'}}) {  
            my $handle = $_->{'number'}->{'pocHandle'};
            if($handle ne $parentHandle) {
                print "The parent handle $parentHandle doesn't match";
                print "the handle $handle in the phone element\n";
                exit;
            }
        }
    }
    elsif(ref($phones->{'phone'}) =~ m/HASH/) { 
        my $handle = $phones->{'phone'}->{'number'}->{'pocHandle'};
        if($handle ne $parentHandle) {
            print "The parent handle $parentHandle doesn't match";
            print "the handle $handle in the phone element\n";
            exit;
        }
    }
    else { 
        print "Unexpected value when received an email element to parse.\n";
        print Dumper $phones; 
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

            push @{$self->{BUFFER}->{$tableToUpdate}}, \%tmpHash;
        }

#        print Dumper $self->{BUFFER}->{$tableToUpdate};
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
        
        push @{$self->{BUFFER}->{$tableToUpdate}}, \%tmpHash;
        return 1;
    }
    elsif(!defined($pocLinks->{'pocLink'})) {return 1;}
    else {
        print "Unexpected value when received a pocLinks element to parse.\n";
        print Dumper $pocLinks; 
       
        return 0;
    }
}#END addPocLinks

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
        print "Unexpected value when received an address to parse.\n";
        print Dumper $addressToParse; 
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

#    print "-----------------------------\n";
#        print 
#            "Rw:    $rowToPush\n".
#            "El:    $elementToParse\n".
#            "Sel:   $subElementToParse\n";
#    print "-----------------------------\n";

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
                    print "Unexpected element $subElementToParse for $elementToParse\n";
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
                    print "Add phones to a table\n";
                }
                else {
                    print "Unexpected element $subElementToParse for $elementToParse\n";
                    exit;
                }
            }
        }
        else {
            print "parsingFunctionChooser: Unable to find a function to parse $subElementToParse that belongs to $elementToParse\n";
            exit;
        }
    }

    return $result;
}#END parsingFunctionChooser

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Set or get the size of the buffer for InsertManager.
sub bufferSize {
    my $self = shift;
    my $bufferSize = shift;
    $self->{BUFFER_SIZE} = ($bufferSize) ? $bufferSize : return $self->{BUFFER_SIZE};
}


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Set or get the default key to look for when parsing element text
# from an XML::Simple hash. 
sub defaultElementTextKey {
    my $self = shift;
    my $key = shift;
    $self->{DEFAULT_ELEMENT_TEXT_KEY} = ($key) ? $key : return $self->{DEFAULT_ELEMENT_TEXT_KEY};
}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
sub dumpBuffer {
    my $self = shift;
    print Dumper $self->{BUFFER};
}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# insertAndFlushBuffer will take the contents of the
# buffer and make use of the schema object to dump its 
# contents to the sql database. Once it has completed 
# dumping the contents the table in the buffer is emptied
# such that there is only the column row in the buffer table.
#
#   @param the table in the buffer to flush to the database 
#       passing in a null value assumes the flushing of the
#       entire buffer.
sub insertAndFlushBuffer {
    my $self = shift;
    my $bufferTableToFlush = shift;
    $bufferTableToFlush = ($bufferTableToFlush) ? $bufferTableToFlush : 0;

#    print "Flushing Buffer\n";

    #print Dumper $self->{BUFFER};

    #If a table is defined then flush only that table.
    #Otherwise flush every table in the buffer.
    if($bufferTableToFlush) { 
        $self->{SCHEMA_OBJECT}->resultset($bufferTableToFlush)->populate(
                $self->{BUFFER}->{$bufferTableToFlush}
            );
        $self->{BUFFER}->{$bufferTableToFlush} = [];
    }
    else {
        foreach my $table (keys $self->{BUFFER}) {
            $self->{SCHEMA_OBJECT}->resultset($table)->populate(
                $self->{BUFFER}->{$table}
            );
            $self->{BUFFER}->{$table} = [];
        }
    }
}

return 1;




