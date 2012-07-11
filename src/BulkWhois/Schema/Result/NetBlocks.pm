#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# This perl module represents the table that binds the Pocs to 
# their respective Asns.
#
package BulkWhois::Schema::Result::NetBlocks;
use base qw/DBIx::Class::Core/;

use strict;
use warnings;
use Data::Dumper;

__PACKAGE__->table('NetBlocks'); #Set the table.

__PACKAGE__->add_columns(
    'netHandle'         => {data_type => 'varchar',     size => 255,    is_nullable => 0},
    'cidrLength'        => {data_type => 'int',                         is_nullable => 0},
    'type'              => {data_type => 'varchar',     size => 23,     is_nullable => 1},
    'startAddress'	=> {data_type => 'varchar',	size => 127,	is_nullable => 1},
    'endAddress'        => {data_type => 'varchar',     size => 127,    is_nullable => 1}
);



return 1;

