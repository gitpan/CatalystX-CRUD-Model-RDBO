package CatalystX::CRUD::Model::RDBO;
use strict;
use warnings;
use base qw( CatalystX::CRUD::Model );
use CatalystX::CRUD::Iterator;
use Sort::SQL;

our $VERSION = '0.03';

__PACKAGE__->mk_ro_accessors(qw( name manager ));
__PACKAGE__->config->{object_class} = 'CatalystX::CRUD::Object::RDBO';

# uncomment these to see the SQL print on stderr
#$Rose::DB::Object::QueryBuilder::Debug = 1;
#$Rose::DB::Object::Debug = 1;

=head1 NAME

CatalystX::CRUD::Model::RDBO - Rose::DB::Object CRUD

=head1 SYNOPSIS

 package MyApp::Model::Foo;
 use base qw( CatalystX::CRUD::Model::RDBO );
 __PACKAGE__->config( 
            name            => 'My::RDBO::Foo', 
            manager         => 'My::RDBO::Foo::Manager',
            load_with       => [qw( bar )],
            page_size       => 50,
            );
 1;

=head1 DESCRIPTION

CatalystX::CRUD::Model::RDBO is a CatalystX::CRUD implementation for Rose::DB::Object.

=head1 CONFIGURATION

The config options can be set as in the SYNOPSIS example.

=head1 METHODS

=head2 name

The name of the Rose::DB::Object-based class that the model represents.
Accessible via name() or config->{name}.

=head2 manager

If C<manager> is not defined in config(),
the Xsetup() method will attempt to load a class
named with the C<name> value from config() 
with C<::Manager> appended.
This assumes the namespace convention of Rose::DB::Object::Manager.

If there is no such module in your @INC path, then
the fall-back default is Rose::DB::Object::Manager.

=cut

=head2 Xsetup

Implements the required Xsetup() method. Instatiates the model's
name() and manager() values based on config().

=cut

sub Xsetup {
    my $self = shift;

    $self->NEXT::Xsetup(@_);

    $self->{name} = $self->config->{name};
    if ( !$self->name ) {
        return if $self->throw_error("need to configure a Rose class name");
    }

    $self->{manager} = $self->config->{manager} || $self->name . '::Manager';

    my $name = $self->name;
    my $mgr  = $self->manager;

    eval "require $name";
    if ($@) {
        return if $self->throw_error($@);
    }

    # what kind of db driver are we using. makes a difference in make_query().
    my $db = $name->new->db;
    $self->{_db_driver} = $db->driver;

    eval "require $mgr";

    # don't fret -- just use RDBO::Manager
    if ($@) {
        $self->{manager} = 'Rose::DB::Object::Manager';
        require Rose::DB::Object::Manager;
    }
}

=head2 new_object( @param )

Returns a CatalystX::CRUD::Object::RDBO object.

=cut

sub new_object {
    my $self = shift;
    my $rdbo = $self->name;
    my $obj;
    eval { $obj = $rdbo->new(@_) };
    if ( $@ or !$obj ) {
        my $err = defined($obj) ? $obj->error : $@;
        return if $self->throw_error("can't create new $rdbo object: $err");
    }
    return $self->NEXT::new_object( delegate => $obj );
}

=head2 fetch( @params )

If present,
@I<params> is passed directly to name()'s new() method,
and is expected to be an array of key/value pairs.
Then the load() method is called on the resulting object.

If @I<params> are not present, the new() object is simply returned,
which is equivalent to calling new_object().

All the methods called within fetch() are wrapped in an eval()
and sanity checked afterwards. If there are any errors,
throw_error() is called.

Example:

 my $foo = $c->model('Foo')->fetch( id => 1234 );
 if (@{ $c->error })
 {
    # do something to deal with the error
 }
 
B<NOTE:> If the object's presence in the database is questionable,
your controller code may want to use new_object() and then call 
load_speculative() yourself. Example:

 my $foo = $c->model('Foo')->new_object( id => 1234 );
 $foo->load_speculative;
 if ($foo->not_found)
 {
   # do something
 }

=cut

sub fetch {
    my $self = shift;
    my $obj = $self->new_object(@_) or return;

    if (@_) {
        my %v = @_;
        my $ret;
        my $name = $self->name;
        my @arg  = ();
        if ( $self->config->{load_with} ) {
            push( @arg, with => $self->config->{load_with} );
        }
        eval { $ret = $obj->read(@arg); };
        if ( $@ or !$ret ) {
            return
                if $self->throw_error( join( " : ", $@, "no such $name" ) );
        }

        # special handling of fetching
        # e.g. Catalyst::Plugin::Session::Store::DBI records.
        if ( $v{id} ) {

            # stringify in case it's a char instead of int
            # as is the case with session ids
            my $pid = $obj->delegate->id;
            $pid =~ s,\s+$,,;
            unless ( $pid eq $v{id} ) {

                return
                    if $self->throw_error(
                          "Error fetching correct id:\nfetched: $v{id} "
                        . length( $v{id} )
                        . "\nbut got: $pid"
                        . length($pid) );
            }
        }
    }

    return $obj;
}

=head2 search( @params )

@I<params> is passed directly to the Manager get_objects() method.
See the Rose::DB::Object::Manager documentation.

Returns an array or array ref (based on wantarray) of 
CatalystX::CRUD::Object::RDBO objects.

=cut

sub search {
    my $self = shift;
    my $objs = $self->_get_objects( 'get_objects', @_ );

    # save ourselves lots of method-call overhead.
    my $class = $self->object_class;

    my @wrapped = map { $class->new( delegate => $_ ) } @$objs;
    return wantarray ? \@wrapped : @wrapped;
}

=head2 count( @params )

@I<params> is passed directly to the Manager get_objects_count() method.
See the Rose::DB::Object::Manager documentation.

Returns an integer.

=cut

sub count {
    my $self = shift;
    return $self->_get_objects( 'get_objects_count', @_ );
}

=head2 iterator( @params )

@I<params> is passed directly to the Manager get_objects_iterator() method.
See the Rose::DB::Object::Manager documentation.

Returns a CatalystX::CRUD::Iterator object whose next() method
will return a CatalystX::CRUD::Object::RDBO object.

=cut

sub iterator {
    my $self = shift;
    my $iter = $self->_get_objects( 'get_objects_iterator', @_ );
    return CatalystX::CRUD::Iterator->new( $iter, $self->object_class );
}

=head2 make_query( I<field_names> )

Implement a RDBO-specific query factory based on request parameters.
Return value can be passed directly to search(), iterator() or count() as
documented in the CatalystX::CRUD::Model API.

I<field_names> should be an array of valid form field names.

The following reserved request param names are implemented:

=over

=item order

Sort order. Should be a SQL-friendly string parse-able by Sort::SQL.

=item page_size

For the Data::Pageset pager object. Defaults to page_size(). An upper limit of 200
is implemented by default to reduce the risk of a user [unwittingly] creating a denial
of service situation.

=item page

What page the current request is coming from. Used to set the offset value
in the query. Defaults to C<1>.

=back

=cut

sub make_query {
    my $self        = shift;
    my $c           = $self->context;
    my $field_names = shift or $self->throw_error("field_names required");

    my $roseq = $self->_rose_query($field_names);
    my $s     = $c->req->param('order') || 'id DESC';
    my $sp    = Sort::SQL->string2array($s);

    # dis-ambiguate common column names
    $s =~ s,\bname\ ,t1.name ,;
    $s =~ s,\bid\ ,t1.id ,;

    # Rose requires ASC/DESC be UPPER case
    $s =~ s,\b(asc|desc)\b,uc($1),eg;

    my $page_size = $c->request->param('page_size') || $self->page_size;
    $page_size = 200 if $page_size > 200;    # don't let users DoS us.
    my $page = $c->req->param('page') || 1;

    my %query = (
        query           => $roseq->{sql},
        sort_by         => $s,
        limit           => $page_size,
        offset          => ( $page - 1 ) * $page_size,
        sort_order      => $sp,
        plain_query     => $roseq->{query},
        plain_query_str => $self->_plain_query_str( $roseq->{query} ),
    );

    return \%query;
}

sub _plain_query_str {
    my ( $self, $q ) = @_;
    my @s;
    for my $p ( sort keys %$q ) {
        my @v = @{ $q->{$p} };
        next unless grep {m/\S/} @v;
        push( @s, "$p = " . join( ' or ', @v ) );
    }
    return join( ' AND ', @s );
}

# make a RDBO-compatible query
sub _rose_query {
    my ( $self, $field_names ) = @_;
    my $c = $self->context;
    my ( @sql, %query );

    # LIKE syntax varies between db implementations
    my $is_ilike = 0;
    if ( $self->{_db_driver} eq 'pg' ) {
        $is_ilike = 1;
    }

    for my $p (@$field_names) {

        next unless exists $c->req->params->{$p};
        my @v    = $c->req->param($p);
        my @safe = @v;
        next unless grep {/./} @safe;

        $query{$p} = \@v;

        # normalize wildcards and set sql accordingly
        if ( grep {/[\%\*]|^!/} @v ) {
            grep {s/\*/\%/g} @safe;
            my @wild = grep {m/\%/} @safe;
            if (@wild) {
                if ($is_ilike) {
                    push( @sql, ( $p => { ilike => \@wild } ) );
                }
                else {
                    push( @sql, ( $p => { like => \@wild } ) );
                }
            }

            # allow for negation of query
            my @not = grep {m/^!/} @safe;
            if (@not) {
                push( @sql, ( $p => { ne => [ grep {s/^!//} @not ] } ) );
            }
        }
        else {
            push( @sql, $p => [@safe] );
        }
    }

    return { sql => \@sql, query => \%query };
}

sub _get_objects {
    my $self    = shift;
    my $method  = shift || 'get_objects';
    my @args    = @_;
    my $manager = $self->manager;
    my $name    = $self->name;
    my @params  = ( object_class => $name );    # not $self->object_class

    if ( ref $args[0] eq 'HASH' ) {
        push( @params, %{ $args[0] } );
    }
    elsif ( ref $args[0] eq 'ARRAY' ) {
        push( @params, @{ $args[0] } );
    }
    else {
        push( @params, @args );
    }

    push(
        @params,
        with_objects  => $self->config->{load_with},
        multi_many_ok => 1
    ) if $self->config->{load_with};

    return $manager->$method(@params);
}

1;

__END__

=head1 AUTHOR

Peter Karman, C<< <karman at cpan.org> >>

=head1 BUGS

Please report any bugs or feature requests to
C<bug-catalystx-crud-model-rdbo at rt.cpan.org>, or through the web interface at
L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=CatalystX-CRUD-Model-RDBO>.
I will be notified, and then you'll automatically be notified of progress on
your bug as I make changes.

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc CatalystX::CRUD::Model::RDBO

You can also look for information at:

=over 4

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/CatalystX-CRUD-Model-RDBO>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/CatalystX-CRUD-Model-RDBO>

=item * RT: CPAN's request tracker

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=CatalystX-CRUD-Model-RDBO>

=item * Search CPAN

L<http://search.cpan.org/dist/CatalystX-CRUD-Model-RDBO>

=back

=head1 ACKNOWLEDGEMENTS

This module is based on Catalyst::Model::RDBO by the same author.

=head1 COPYRIGHT & LICENSE

Copyright 2007 Peter Karman, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

=cut
