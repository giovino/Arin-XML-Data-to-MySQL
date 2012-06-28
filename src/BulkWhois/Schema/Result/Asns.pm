#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# This class represents the asns table in the the database. 
# @TODO test this module and make sure it works.
#
package BulkWhois::Schema::Result::Asns; #@NOTE All modules need to be in the following path Result/Asns.
use base qw/DBIx::Class::Core/; #Inherit from the parent class? or does this simply 
                                # make its functions usable.

use strict;
use warnings;
use Data::Dumper;

#print Dumper __PACKAGE__; 
#print Dumper BulkWhois::; exit;


#Set the table the module will represent.
__PACKAGE__->table('Asns');

#This table contains columns that store DATETIME vars.
# This component will convert this into a perl object.
__PACKAGE__->load_components("InflateColumn::DateTime");

#Adds em collumns. 
__PACKAGE__->add_columns(
    'asnHandle'         => {data_type => 'varchar',     size => 255,    is_nullable => 0},
    'ref'               => {data_type => 'varchar',     size => 512,    is_nullable => 1},
    'startAsNumber'     => {data_type => 'int',         is_nullable => 1},
    'endAsNumber'       => {data_type => 'int',         is_nullable => 1},
    'name'              => {data_type => 'varchar',     size => 255,    is_nullable => 1},
    'registrationDate'  => {data_type => 'datetime',    is_nullable => 1},
    'updateDate'        => {data_type => 'datetime',    is_nullable => 1},
    'comment'           => {data_type => 'varchar',     size => 4097,     is_nullable => 1} 
);

#Now set the primary key. This function will take in an array of pk's.
__PACKAGE__->set_primary_key('asnHandle');

#Now make sure non of the asn handles never match. Actually cannot use this feature 
# if the user whishes to keep an update history.
__PACKAGE__->add_unique_constraint('asnHandle', ['asnHandle']);

return 1;
