#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# This module uses the sax parser to dump the BulkWhois xml file to a database.
#
package InsertManager::SAXInsertManager;
use base qw/InsertManager::InsertManagerInterface/;
use base qw/XML::SAX::Base/;
use InsertManager::Mappings;

use strict;
use warnings;
use Data::Dumper;
use BulkWhois::Schema;
use Scalar::Util 'blessed';
use XML::SAX;
#use JSON;
#use Switch;

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Create a new SAXInsertManager. 
#   
#   @param bufferSize => 'the buffer size'. The maximum size 
#       of the buffer before a bulk insert is performed.
#   @param schema => $schemaObject. A refernce to the
#       DBIx::Class::Schema object. This will be used 
#       to perform the inserts on.
sub new {
    #Get the argumensts
    my $class = shift;
    my %args = @_;
    my $bufferSize  = ($args{'bufferSize'}) ? $args{'bufferSize'} : 10;
    #Make sure a schema object is passed in. Otherwise die.
    my $schemaObj   = (blessed($args{'schema'}) && (blessed($args{'schema'}) eq "BulkWhois::Schema")) 
                    ? $args{'schema'} 
                    : die "I need a schema object, Create one and pass it in.\n";
    my $self->{ITEMS_PROCESSED} = 0;

    my $buffer = InsertManager::BufferManager->new(bufferSize => $bufferSize, schema => $schemaObj);
    $self->{BUFFER} = $buffer; #Stores the buffer object

    #Perform the blessing
    bless $self, $class;

    $self->{PARSER} = XML::SAX::ParserFactory->parser( 
        Handler => $self 
    );

    #DEBUGGING and LEARNING
#    $self->information;   

    return $self;
}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Takes in a single element (asn, org, poc, net) calls the 
# appropriate fucntions to parse it.
#
#   @param the xml element to parse.
sub parseXML {
    my $self = shift;
    my $xmlElement = shift;
    
    #So it doesn't kill the appliction
    eval {$self->{PARSER}->parse_string($xmlElement)};
    #TODO add error reporting for eval

    exit;     
}

#Since individual asn, poc, org, and net elements are used
# they are treated as the root element.
sub start_document {
    my ($self, $doc) = @_;
    
    #process document start event
    print "Start document\n";
}

sub end_document {
    my ($self, $doc) = @_;
    print "Stop document\n";
}

#Called at the start of an element
sub start_element {
    my ($self, $el) = @_;
    # process element start event
    print "Element: ". $$el{Name}."\n";
}

#Get the contents of the element.
sub characters {
    my ($self, $chars) = @_;
    print Dumper $chars;
}

sub end_element {
    my ($self, $el) = @_;
}

sub information {
    my $self = shift;

    print "Parsers: ". Dumper(XML::SAX->parsers());

    exit;
}

return 1;
