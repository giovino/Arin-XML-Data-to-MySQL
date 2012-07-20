#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# This class manages a buffer to use with DBIx. Hopefully performing 
# bulk inserts will reduce the time to parse a huge XML file.
#
# @TODO Enable the ability to set buffers for individual tables (I'll probably
#  allow the setting of a boolean variable that will make use of a hash in 
#  Mappings.pm
package InsertManager::BufferManager;
use base qw/InsertManager::InsertManagerInterface/;
use InsertManager::Mappings;

use strict;
use warnings;
use Scalar::Util 'blessed';
use Data::Dumper;

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Create a new BufferManager. 
#   
#   @param bufferSize => 'the buffer size'. The maximum size 
#       of the buffer before a bulk insert is performed.
#   @param schema => $schemaObject. A refernce to the
#       DBIx::Class::Schema object. This will be used 
#       to perform the inserts on.
#   @param logger => $loggerObject. A refernce to a 
#       log4perl::Logger object.
#
sub new {
    #Get the argumensts
    my $class = shift;
    my %args = @_;
    my $bufferSize  = ($args{'bufferSize'}) ? $args{'bufferSize'} : 10;
    #Make sure a schema object is passed in. Otherwise die.
    my $schemaObj   = (blessed($args{'schema'}) && (blessed($args{'schema'}) eq "BulkWhois::Schema")) 
                    ? $args{'schema'} 
                    : die "I need a schema object, Create one and pass it in.\n";
    my $logger   = (blessed($args{'logger'}) && (blessed($args{'logger'}) eq "Log::Log4perl::Logger")) 
                    ? $args{'logger'} 
                    : die "I need a log4perl object, Create one and pass it in.\n";
    my $self->{MAX_BUFFER_SIZE} = $bufferSize; 
    $self->{SCHEMA_OBJECT} = $schemaObj; 
    $self->{BUFFER} = {};
    $self->{CURRENT_BUFFER_SIZE} = 0;
    $self->{CURRENT_SUB_BUFFER_SIZES} = {};
    $self->{LOGGER} = $logger;

    #Initialize the buffer with all of the sub buffers for the tables.
    #The format will be key => array pairs.
    while( my ($key, $columns) = each($TABLES))  {
        $self->{CURRENT_SUB_BUFFER_SIZES}->{$key} = 0;
        $self->{BUFFER}->{$key} = [];
    }


    #Perform the blessing
    bless $self, $class;

    return $self;
}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Add an item to the buffer. If a buffer reaches its
# maximum capacity then dump the contents to the database.
#
#   @param the table the item belongs to
#   @param the item to push
sub pushToBuffer {
    my $self = shift;
    my $table = shift;
    my @items = @_;
    
#    #TODO detect if $item is a hash or an array so that i can properly 
#    #increment the counters.
#    print "Dumping to table: $table\n";
#    print "The number of items is: ". @items ."\n";
#    print Dumper @items;
#    print ref(\@items)."\n"; exit;
    

    push @{$self->{BUFFER}->{$table}}, @items;
    $self->{CURRENT_SUB_BUFFER_SIZES}->{$table} += @items;
    $self->{CURRENT_BUFFER_SIZE} += @items;
    
#    print "Sub Bs: ".$self->{CURRENT_SUB_BUFFER_SIZES}->{$table}."\n";
#    print "B: ". $self->{CURRENT_BUFFER_SIZE}."\n";exit;

    # Perform extra work if the subbuffer reaches its maximum capacity
    if($self->{CURRENT_SUB_BUFFER_SIZES}->{$table} == $self->{MAX_BUFFER_SIZE}) {
#        print "Buffer capacity for $table reached. Dumping to database.";
        $self->insertAndFlushBuffer($table);
#        print "Done\n";

        $self->{CURRENT_SUB_BUFFER_SIZES}->{$table} = 0;
        $self->{CURRENT_BUFFER_SIZE} -= $self->{MAX_BUFFER_SIZE};
    }
}


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Set or get the size of the buffer for InsertManager.
sub bufferSize {
    my $self = shift;
    my $bufferSize = shift;
    $self->{MAX_BUFFER_SIZE} = ($bufferSize) ? $bufferSize : return $self->{MAX_BUFFER_SIZE};
}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Prints the buffer to the screen.
sub dumpBuffer {
    my $self = shift;
    my $subBuffToDump = shift;
    if(!$subBuffToDump) {
        print Dumper $self->{BUFFER};
    }
    else {
        print Dumper $self->{BUFFER}->{$subBuffToDump};
    }
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


