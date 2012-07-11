#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# This perl module represents the table that binds the Pocs to 
# their respective Asns.
#
package BulkWhois::Schema::Result::OriginASes;
use base qw/DBIx::Class::Core/;

use strict;
use warnings;
use Data::Dumper;

__PACKAGE__->table('OriginASes'); #Set the table.

__PACKAGE__->add_columns(
    'netHandle'         => {data_type => 'varchar',     size => 255,    is_nullable => 0},
    'originAS'          => {data_type => 'varchar',     size => 255,    is_nullable => 0}
);



return 1;

