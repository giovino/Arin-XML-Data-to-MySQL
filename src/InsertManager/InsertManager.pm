#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# The InsertManager class takes in an xml hash related
# to the whois database and performs an insert to the proper
# table. This elimates all the nasty details. A nice feature
# to InsertManager is that it will buffer up all the inserts
# and then perform a bulk insert.
package InsertManager::InsertManager;

use strict;
use warnings;
use Data::Dumper;
use BulkWhois::Schema;
use Scalar::Util 'blessed';

#These are the tables that InsertManager will expect to find in the DBIx::Class object.
# If you add a new table make sure to update this hash. If you update a column name or 
# add a new column update this hash. AKA reflect any changes made in this hash.
# NOTE If the xml data is not in a string format then you may need to add an exception
# in $COLUMNS_THAT_NEED_EXTRA_PARSING hash for extra parsing. (you will need to create a 
# function also)
my $TABLES = {
    'Asns' => [qw/asnHandle orgHandle ref startAsNumber endAsNumber name registrationDate updateDate comment/],
    'Asns_Pocs' => [qw/asnHandle pocHandle function description/] #The handles need to be in the same order as the table naming scheme. In this case the asnHandle is first and the pocHandle is second. 
};

#Some elements in the xml file can be parsed into a single column. For example the 
#comments element in the BulkWhois XML dump doesn't need to be seperated into lines.
#This hash contains keys of all the main BulkWhois elements sub elements that will need
#extra processsing.
my $ELEMENTS_THAT_NEED_EXTRA_PARSING = {
    'asn' => {
        comment => 1,
        pocLinks => 1
    }
};

#Allows InsertManager to recognize xml elements and attributes from an XML::Simple hash
# with their corresponding column entries in the database. 
my $COLUMN_TO_XML_MAPPINGS = {
    'Asns' => {
        $TABLES->{'Asns'}->[0] => 'handle',
        $TABLES->{'Asns'}->[1] => 'orgHandle',
        $TABLES->{'Asns'}->[2] => 'ref',
        $TABLES->{'Asns'}->[3] => 'startAsNumber',
        $TABLES->{'Asns'}->[4] => 'endAsNumber',
        $TABLES->{'Asns'}->[5] => 'name',
        $TABLES->{'Asns'}->[6] => 'registrationDate',
        $TABLES->{'Asns'}->[7] => 'updateDate',
        $TABLES->{'Asns'}->[8] => 'comment'
    },
    'Asns_Pocs' => {
        $TABLES->{'Asns_Pocs'}->[0] => 'asnHandle',
        $TABLES->{'Asns_Pocs'}->[1] => 'handle',
        $TABLES->{'Asns_Pocs'}->[2] => 'function',
        $TABLES->{'Asns_Pocs'}->[3] => 'description'
    }
};

#The inverse of COLUMN_TO_XML_MAPPINGS
my $XML_TO_COLUMN_MAPPINGS = {
    'Asns' => reverse $COLUMN_TO_XML_MAPPINGS->{'Asns_Pocs'},
    'Asns_Pocs' => reverse $COLUMN_TO_XML_MAPPINGS->{'Asns'}
};

#The default key to look for when finding the value of an element in an
#XML::Simple hash.
my $DEFAULT_ELEMENT_TEXT_KEY = undef;

#print "Dump of MAP " . Dumper $XML_TO_COLUMN_MAPPINGS;exit;

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Create a new InsertManager. The 
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
            #push @{$self->{BUFFER}->{'Asns'}}, $self->asnsAddRow($value);
            #if(@{$self->{BUFFER}->{'Asns'}} == $self->{BUFFER_SIZE}) {
            #    $self->insertAndFlushBuffer('Asns');
            #    $self->insertAndFlushBuffer('Asns_Pocs');
            #}
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
}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Formats data into an array so that it can be inserted into
# the asns table. Makes sure that the elements of the 
# simple hash are ordered to match the ordering of the 
# columns in the asns table. Refer to the constants 
# hash at the top of the file if you wish to change the
# ordering.
#
#   @param a row to push into the database. The expected variable type
#       is a hash that has been produced by XML::Simple::XMLin(...)
#
#   @return a hash that can be used by the populate method in DBIx::Class::Schema object.
sub asnsAddRow {
    my $self = shift;
    my $rowToPush = shift;

    my %tmpHash = ();
    #parse all of the simple elements in the hash.
    foreach(@{$TABLES->{'Asns'}}) {
        my $corresXMLElement = ${$COLUMN_TO_XML_MAPPINGS->{'Asns'}}{$_}; #Get the column that corresponds to the xml element.

        
        if(!$ELEMENTS_THAT_NEED_EXTRA_PARSING->{'asn'}->{$corresXMLElement}) {
            $tmpHash{$_} = $rowToPush->{$corresXMLElement};
        }
    }

    #Go through all of the elements that need extra parsing
    foreach my $key (keys $ELEMENTS_THAT_NEED_EXTRA_PARSING->{'asn'}) {
        my $column = $XML_TO_COLUMN_MAPPINGS->{'Asns'}->{$key};
        

#        print "Key: ". Dumper $key;
#        print "Column: ". Dumper $column;
#        print "Row: " . Dumper $rowToPush;


        #If there is a mapping then call the parsingFunctionChooser and 
        #push the result into the tmpHash.
        if(defined($column)) {    
            $tmpHash{$column} = $self->parsingFunctionChooser($rowToPush, 'asn', $column);
        }
        #If column is null then call the parsingFunctionChooser and let it call 
        #a function to handle the data properly.
        else {
            $self->parsingFunctionChooser($rowToPush, 'asn', $key);
        }
    }

    return \%tmpHash;
}

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
}

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
        print "Unexpected value when received an address to parse.\n";
        print Dumper $commentToParse; 
    }

    return $comment;
}

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
    #print "Args: ". Dumper \@_; exit;

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
    if($elementToParse eq 'asn') {
        if($subElementToParse eq 'comment') {
            $result = $self->commentToString($rowToPush->{$subElementToParse});
        }
        elsif($subElementToParse eq 'pocLinks') {
            $result = $self->addPocLinks('Asns_Pocs', $rowToPush->{'handle'}, $rowToPush->{$subElementToParse});
        }
        else {
            print "Unexpected element $subElementToParse\n";
        }
    }
    else {
        print "parsingFunctionChooser: Undable to find a function to parse $elementToParse that belongs to $subElementToParse\n";
        exit;
    }

    return $result;
}

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




