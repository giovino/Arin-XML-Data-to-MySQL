#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# This perl module represents the table that binds the Pocs to 
# their respective Asns.
#
package BulkWhois::Schema::Result::Pocs_Nets;
use base qw/DBIx::Class::Core/;

use strict;
use warnings;
use Data::Dumper;

__PACKAGE__->table('Pocs_Nets'); #Set the table.

__PACKAGE__->add_columns(
    'pocHandle'         => {data_type => 'varchar',     size => 255,    is_nullable => 0},
    'netHandle'         => {data_type => 'varchar',     size => 255,    is_nullable => 0},
    'limit'             => {data_type => 'int',         is_nullable => 1},
    'limitExceeded'     => {data_type => 'boolean',     is_nullable => 1}
);



return 1;

