#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# This perl module represents the table that binds the Pocs to 
# their respective Asns.
#
package BulkWhois::Schema::Result::Pocs_Phones;
use base qw/DBIx::Class::Core/;

use strict;
use warnings;
use Data::Dumper;

__PACKAGE__->table('Pocs_Phones'); #Set the table.

__PACKAGE__->add_columns(
    'pocHandle'     => {data_type => 'varchar',     size => 255,    is_nullable => 0},
    'phoneNumber'   => {data_type => 'varchar',     size => 127,    is_nullable => 0},
    'phoneType'     => {data_type => 'varchar',     size => 31,     is_nullable => 1}
);



return 1;

