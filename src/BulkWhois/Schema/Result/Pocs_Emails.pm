#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# This perl module represents the table that binds the Pocs to 
# their respective Asns.
#
package BulkWhois::Schema::Result::Pocs_Emails;
use base qw/DBIx::Class::Core/;

use strict;
use warnings;
use Data::Dumper;

__PACKAGE__->table('Pocs_Emails'); #Set the table.

__PACKAGE__->add_columns(
    'pocHandle'     => {data_type => 'varchar',     size => 255,    is_nullable => 0},
    'email'         => {data_type => 'varchar',     size => 255,    is_nullable => 0}
);



return 1;

