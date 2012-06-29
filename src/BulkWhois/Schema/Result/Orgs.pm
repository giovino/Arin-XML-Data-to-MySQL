#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# This class represents the asns table in the the database. 
# @TODO test this module and make sure it works.
#
package BulkWhois::Schema::Result::Orgs;
use base qw/DBIx::Class::Core/; #Inherit from the parent class.

use strict;
use warnings;

#Set the table the module will represent.
__PACKAGE__->table('Orgs');

#This table contains columns that store DATETIME vars.
# This component will convert this into a perl object.
__PACKAGE__->load_components("InflateColumn::DateTime");

#Adds em collumns. 
__PACKAGE__->add_columns(
    'orgHandle'         => {data_type => 'varchar',     size => 255,    is_nullable => 0},
    'registrationDate'  => {data_type => 'datetime',    is_nullable => 1},
    'ref'               => {data_type => 'varchar',     size => 511,    is_nullable => 1},
    'city'	        => {data_type => 'varchar',     size => 63,     is_nullable => 1},
    'iso3166-1'		=> {data_type => 'varchar',	size => 511,	is_nullable => 1},
    'name'		=> {data_type => 'varchar',	size => 255,	is_nullable => 1},
    'postalCode'	=> {data_type => 'varchar',	size => 255,	is_nullable => 1},
    'iso3166-2'		=> {data_type => 'varchar',	size => 511,	is_nullable => 1},
    'updateDate'        => {data_type => 'datetime',    is_nullable => 1},
    'addressHandle'	=> {data_type => 'int',		is_nullable => 1},
    'comments'		=> {data_type => 'varchar',	size => 4097,	is_nullable => 1}
);

#Now set the primary key. This function will take in an array of pk's.
__PACKAGE__->set_primary_key('orgHandle');

#Now make sure non of the asn handles never match. 
__PACKAGE__->add_unique_constraint('orgHandle', ['orgHandle']);

return 1;
