package InsertManager::BulkWhoisSAXHandler;
use base qw(XML::SAX::Base);

#Default constructor
sub new {
    my $class = shift;
    my $self->{BLARG} = 0;

    bless $self, $class;

    return $self;
}

sub start_document {
    my ($self, $doc) = @_;
        #process document start event
        print "Start document\n";
}

sub start_element {
    my ($self, $el) = @_;
        # process element start event
        print "start element\n";
}



return 1;


