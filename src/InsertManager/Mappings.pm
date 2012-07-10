#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# This file contains all of the mappings between the tables in the
# database and the xml elements in the bulkwhois file.
#
#
package InsertManager::Mappings;

use warnings;
use strict;
use Data::Dumper;

use base 'Exporter';
our @EXPORT = qw($TABLES $ELEMENTS_THAT_NEED_EXTRA_PARSING 
                $COLUMN_TO_XML_MAPPINGS $XML_TO_COLUMN_MAPPINGS);

#These are the tables that InsertManager will expect to find in the DBIx::Class object.
# If you add a new table make sure to update this hash. If you update a column name or 
# add a new column update this hash. AKA reflect any changes made in this hash.
# NOTE If the xml data is not in a string format then you may need to add an exception
# in $COLUMNS_THAT_NEED_EXTRA_PARSING hash for extra parsing. (you will need to create a 
# function also)
our $TABLES = {
    'Asns'      => [qw/asnHandle orgHandle ref startAsNumber endAsNumber name registrationDate updateDate comment/],
    'Asns_Pocs' => [qw/asnHandle pocHandle function description/], #The handles need to be in the same order as the table naming scheme. In this case the asnHandle is first and the pocHandle is second. 
    'Pocs'      => [qw/pocHandle ref city registrationDate firstName middleName lastName companyName postalCode updateDate iso3166_1 iso3166_2 isRoleAccount address/],
    'Nets_Pocs' => [qw/pocHandle netHandle/],
    'Pocs_Emails' => [qw/pocHandle email/],
    'Pocs_Phones' => [qw/pocHandle phoneNumber phoneType/]
};

#Some elements in the xml file can be parsed into a single column. For example the 
#comments element in the BulkWhois XML dump doesn't need to be seperated into lines.
#This hash contains keys of all the main BulkWhois elements sub elements that will need
#extra processsing.
our $ELEMENTS_THAT_NEED_EXTRA_PARSING = {
    'asn' => {
        comment     => 1,   #Convert to a string
        pocLinks    => 1    #Add to Asns_Pocs
   },
    'poc' => {
        'iso3166-1'     => 1,   #convert to json
        emails          => 1,   #Add to Pocs_Emails
        isRoleAccount   => 1,   #Convert to a tiny int.
        phones          => 1,   #Add to a table in the future
        streetAddress   => 1    #Convert to a string.
    }
};

#Allows InsertManager to recognize xml elements and attributes from an XML::Simple hash
# with their corresponding column entries in the database. 
our $COLUMN_TO_XML_MAPPINGS = {
    'Asns' => {
        $TABLES->{'Asns'}->[0] => 'handle',
        $TABLES->{'Asns'}->[1] => 'orgHandle',
        $TABLES->{'Asns'}->[2] => 'ref',
        $TABLES->{'Asns'}->[3] => 'startAsNumber',
        $TABLES->{'Asns'}->[4] => 'endAsNumber',
        $TABLES->{'Asns'}->[5] => 'name',
        $TABLES->{'Asns'}->[6] => 'registrationDate',
        $TABLES->{'Asns'}->[7] => 'updateDate',
        $TABLES->{'Asns'}->[8] => 'comment'
    },
    'Asns_Pocs' => {
        $TABLES->{'Asns_Pocs'}->[0] => 'asnHandle',
        $TABLES->{'Asns_Pocs'}->[1] => 'handle',
        $TABLES->{'Asns_Pocs'}->[2] => 'function',
        $TABLES->{'Asns_Pocs'}->[3] => 'description'
    }, 
    'Pocs' => {
        $TABLES->{'Pocs'}->[0]  => 'handle',
        $TABLES->{'Pocs'}->[1]  => 'ref',
        $TABLES->{'Pocs'}->[2]  => 'city',
        $TABLES->{'Pocs'}->[3]  => 'registrationDate',
        $TABLES->{'Pocs'}->[4]  => 'firstName',
        $TABLES->{'Pocs'}->[5]  => 'middleName',
        $TABLES->{'Pocs'}->[6]  => 'lastName',
        $TABLES->{'Pocs'}->[7]  => 'companyName',
        $TABLES->{'Pocs'}->[8]  => 'postalCode',
        $TABLES->{'Pocs'}->[9]  => 'updateDate',
        $TABLES->{'Pocs'}->[10] => 'iso3166-1',
        $TABLES->{'Pocs'}->[11] => 'iso3166-2',
        $TABLES->{'Pocs'}->[12] => 'isRoleAccount',
        $TABLES->{'Pocs'}->[13] => 'streetAddress'
    },
    #'Pocs_Emails' => [qw/pocHandle email/],
    'Pocs_Emails' => {
        $TABLES->{'Pocs_Emails'}->[0] => 'pocHandle',
        $TABLES->{'Pocs_Emails'}->[1] => 'email'
    }
    #'Pocs_Phones' => [qw/pocHandle phoneNumber phoneType/]
};

our $XML_TO_COLUMN_MAPPINGS = {
    'Asns'          => {reverse %{$COLUMN_TO_XML_MAPPINGS->{'Asns'}}},
    'Asns_Pocs'     => {reverse %{$COLUMN_TO_XML_MAPPINGS->{'Asns_Pocs'}}},
    'Pocs'          => {reverse %{$COLUMN_TO_XML_MAPPINGS->{'Pocs'}}},
    'Pocs_Emails'   => {reverse %{$COLUMN_TO_XML_MAPPINGS->{'Pocs_Emails'}}}
};

return 1;
