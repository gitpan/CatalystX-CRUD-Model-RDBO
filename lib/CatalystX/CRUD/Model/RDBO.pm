package CatalystX::CRUD::Model::RDBO;
use strict;
use warnings;
use base qw( CatalystX::CRUD::Model CatalystX::CRUD::Model::Utils );
use CatalystX::CRUD::Iterator;

our $VERSION = '0.11';

__PACKAGE__->mk_ro_accessors(qw( name manager ));
__PACKAGE__->config->{object_class} = 'CatalystX::CRUD::Object::RDBO';

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
    $self->use_ilike(1) if $db->driver eq 'pg';

    # rdbo sql uses 'ne' for not equal
    $self->ne_sign('ne');

    # load the Manager
    eval "require $mgr";

    # don't fret -- just use RDBO::Manager
    if ($@) {
        $self->{manager} = 'Rose::DB::Object::Manager';
        require Rose::DB::Object::Manager;
    }

    # turn on debugging help
    if ( $ENV{CATALYST_DEBUG} && $ENV{CATALYST_DEBUG} > 1 ) {
        $Rose::DB::Object::QueryBuilder::Debug = 1;
        $Rose::DB::Object::Debug               = 1;
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
    return wantarray ? @wrapped : \@wrapped;
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

See CatalystX::CRUD::Model::Utils::make_sql_query() for API details.

=cut

sub _get_field_names {
    my $self = shift;
    return $self->{_field_names} if $self->{_field_names};
    my @cols = $self->name->meta->column_names;
    $self->{_field_names} = \@cols;
    return \@cols;
}

=head2 treat_like_int

Returns hash ref of all column names that return type =~ m/^date(time)$/.
This is so that wildcard searches for date and datetime-based columns
will get proper SQL rendering.

=cut

sub treat_like_int {
    my $self = shift;
    return $self->{_treat_like_int} if $self->{_treat_like_int};
    $self->{_treat_like_int} = {};
    my $col_names = $self->_get_field_names;

    # treat wildcard timestamps like ints not text (>= instead of ILIKE)
    for my $name (@$col_names) {
        my $col = $self->name->meta->column($name);
        $self->{_treat_like_int}->{$name} = 1
            if $col->type =~ m/^date(time)?$/;
    }

    return $self->{_treat_like_int};
}

sub make_query {
    my $self        = shift;
    my $c           = $self->context;
    my $field_names = shift || $self->_get_field_names;
    my $q           = $self->make_sql_query($field_names);

    # dis-ambiguate common column names
    $q->{sort_by} =~ s,\bname\ ,t1.name ,;
    $q->{sort_by} =~ s,\bid\ ,t1.id ,;

    return $q;
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
