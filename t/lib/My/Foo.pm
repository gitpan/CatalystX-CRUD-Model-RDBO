package My::Foo;
use base qw( Rose::DB::Object );
use Carp;
use Data::Dump qw( dump );

# create a temp db
my $db = Rose::DBx::TestDB->new;

{
    my $dbh = $db->dbh;

    # create a schema to match this class
    $dbh->do(
        "create table foos ( id INTEGER PRIMARY KEY AUTOINCREMENT, name VARCHAR(16) );"
    );

    # create some data
    $dbh->do("insert into foos (name) values ('bar');");

    # double check
    my $sth = $dbh->prepare("SELECT * FROM foos");
    $sth->execute;
    croak "bad seed data in sqlite"
        unless $sth->fetchall_arrayref->[0]->[0] == 1;

    $sth = undef;    # http://rt.cpan.org/Ticket/Display.html?id=22688
                     # does not seem to work.

}

__PACKAGE__->meta->setup(
    table   => 'foos',
    columns => [
        id   => { type => 'serial',  not_null => 1, primary_key => 1 },
        name => { type => 'varchar', length   => 16 },
    ],
);

sub init_db {
    return $db;
}

1;
