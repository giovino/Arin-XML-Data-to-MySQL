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

my $TABLES = {
        'asns' => [qw/asnHandle ref startAsNumber endAsNumber name registrationDate updateDate/]
};
my $XML_TO_COLUMN_MAPPINGS = {
        'asns' => {
            ${$TABLES->{'asns'}}[0] => 'handle',
            ${$TABLES->{'asns'}}[1] => 'ref',
            ${$TABLES->{'asns'}}[2] => 'startAsNumber',
            ${$TABLES->{'asns'}}[3] => 'endAsNumber',
            ${$TABLES->{'asns'}}[4] => 'name',
            ${$TABLES->{'asns'}}[5] => 'registrationDate',
            ${$TABLES->{'asns'}}[6] => 'updateDate'
        }
};

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
    
    $self->{BUFFER} = {};

    #Initialize the buffer with all of the sub buffers for the tables.
    #The format will be key => array pairs.
#    print Dumper $TABLES;
    while( my ($key, $columns) = each($TABLES))  {
        push @{$self->{BUFFER}->{$key}}, $columns;
    }

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
        #Determin which table the hash goes to. Ignore the case
        #of $key
        if($key =~ m/asn/i) {
            push $self->{BUFFER}->{'asns'}, $self->asnsAddRow($value);
        }
        else {
            print Dumper $key, $value;
            print "Unable to find a function that can handle the hash you passed in.\n";
        }
    }
}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Formats data into an array so that it can be inserted into
# the asns table. Makes sure that the elements of the 
# simple hash are ordered to match the ordering of the 
# columns in the asns table. Refer to the constants 
# hash at the top of the file if you wish to change the
# ordering.
sub asnsAddRow {
    my $self = shift;
    my $rowToPush = shift; 

    my @tmpArray = ();
    foreach(@{$TABLES->{'asns'}}) {
        my $mapping = ${$XML_TO_COLUMN_MAPPINGS->{'asns'}}{$_};
        push @tmpArray, $rowToPush->{$mapping}; #Proper syntax? though shall seeeez.
    } 

    return \@tmpArray;
}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Set or get the size of the buffer for InsertManager.
sub bufferSize {
    my $self = shift;
    my $bufferSize = shift;
    $self->{BUFFER_SIZE} = ($bufferSize) ? $bufferSize : return $self->{BUFFER_SIZE};
}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
sub dumpBuffer {
    my $self = shift;
    print Dumper $self->{BUFFER};
}

return 1;




