#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# This perl module represents the table that binds the Pocs to 
# their respective Asns.
#
package BulkWhois::Schema::Result::Orgs_Pocs;
use base qw/DBIx::Class::Core/;

use strict;
use warnings;
use Data::Dumper;

__PACKAGE__->table('Orgs_Pocs'); #Set the table.

__PACKAGE__->add_columns(
    'orgHandle'         => {data_type => 'varchar',     size => 255,    is_nullable => 0},
    'pocHandle'         => {data_type => 'varchar',     size => 255,    is_nullable => 0},
    'function'          => {data_type => 'varchar',     size => 255,    is_nullable => 1},
    'description'       => {data_type => 'varchar',     size => 1023,   is_nullable => 1}
);



return 1;

