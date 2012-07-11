#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# If you wish to replace the default insert manager in ArinXMLParser
# then implement this 'interface'.
# 
#
package InsertManager::InsertManagerInterface;

use warnings;
use strict;

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Parses xml.
#
#   @optional @param xml to parse.
#   @optional @param the name of the element to parse
sub parseXML {
    die "You need to overide this method if you are implementing the interface\n";
}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Implement this method if you wish to perform some tidying up.
sub endParsing {}


return 1;

