#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Schema.pm is the main database component that inherits from 
#  DBIx::Class::Schema. Its task (from what I understand) is to 
#  manage the database. It is the main component that calls the 
#  other modules that inherit from DBIx classes.
package BulkWhois::Schema;
use base qw/DBIx::Class::Schema/;

use BulkWhois::Schema::Asns;
use strict;
use warnings;

#load My::Schema::Result::* #and their result set classes.
__PACKAGE__->load_namespaces();


return 1;


