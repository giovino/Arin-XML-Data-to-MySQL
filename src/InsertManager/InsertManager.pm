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
        'Asns' => [qw/asnHandle ref startAsNumber endAsNumber name registrationDate updateDate/]
};
my $XML_TO_COLUMN_MAPPINGS = {
        'Asns' => {
            $TABLES->{'Asns'}->[0] => 'handle',
            $TABLES->{'Asns'}->[1] => 'ref',
            $TABLES->{'Asns'}->[2] => 'startAsNumber',
            $TABLES->{'Asns'}->[3] => 'endAsNumber',
            $TABLES->{'Asns'}->[4] => 'name',
            $TABLES->{'Asns'}->[5] => 'registrationDate',
            $TABLES->{'Asns'}->[6] => 'updateDate'
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
    $self->{ITEMS_PROCESSED} = 0;
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
        #Determine wich table the hash goes to. Ignore the case
        #of $key
        if($key =~ m/asn/i) {
            #my $bufferSize = @{$self->{BUFFER}->{'Asns'}};
            #print "Buffer Size: $bufferSize / ". $self->{BUFFER_SIZE} ."\n"; 

            push $self->{BUFFER}->{'Asns'}, $self->asnsAddRow($value);
            $self->insertAndFlushBuffer('Asns') if(@{$self->{BUFFER}->{'Asns'}} == $self->{BUFFER_SIZE}); 
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
sub asnsAddRow {
    my $self = shift;
    my $rowToPush = shift; 

    my @tmpArray = ();
    foreach(@{$TABLES->{'Asns'}}) {
        my $mapping = ${$XML_TO_COLUMN_MAPPINGS->{'Asns'}}{$_};
        push @tmpArray, $rowToPush->{$mapping};
    }

    return \@tmpArray;
}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Formats the hash data so that it can be inserted 
# into a table that binds Pocs to Asns
#
#   @param a asnHandle the pocLink belongs to.
#   @param the pocLink
sub asns_Pocs {
   my $asnHandle = shift;
   my $pocLink = shift;
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

    #If a table is defined then flush only that table.
    #Otherwise flush every table in the buffer.
    if($bufferTableToFlush) { 
        $self->{SCHEMA_OBJECT}->resultset($bufferTableToFlush)->populate(
                $self->{BUFFER}->{$bufferTableToFlush}
            );
        $self->{BUFFER}->{$bufferTableToFlush} = 
            [$self->{BUFFER}->{$bufferTableToFlush}->[0]];
    }
    else {
        foreach my $table (keys $self->{BUFFER}) {
            $self->{SCHEMA_OBJECT}->resultset($table)->populate(
                $self->{BUFFER}->{$table}
            );
            $self->{BUFFER}->{$table} = 
                [$self->{BUFFER}->{$table}->[0]];
        }
    }
}

return 1;




