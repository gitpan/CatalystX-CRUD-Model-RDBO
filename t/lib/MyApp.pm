package MyApp;
use Catalyst::Runtime '5.70';
use Catalyst;
use Carp;

our $VERSION = '0.01';

__PACKAGE__->setup();

sub foo : Local {

    my ( $self, $c, @arg ) = @_;

    my $thing = $c->model('Foo')->new_object( id => 1 );

    for my $m (qw( create read update delete)) {
        croak unless $thing->can($m);
    }

    # try fetching our seed data
    $thing->read();

    croak "bad read" unless ( $thing->delegate->name eq 'bar' );

    $c->res->body("foo is a-ok");

}

1;
