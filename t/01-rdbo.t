use Test::More tests => 14;

BEGIN {
    $ENV{CATALYST_DEBUG} = $ENV{PERL_DEBUG} || 0;
    use lib qw( ../../CatalystX-CRUD/trunk/lib );
    use_ok('CatalystX::CRUD::Model::RDBO');
    use_ok('CatalystX::CRUD::Object::RDBO');
    use_ok('Rose::DBx::TestDB');
    use_ok('Rose::DB::Object');
}

use lib qw( t/lib );
use Catalyst::Test 'MyApp';
use Data::Dump qw( dump );
use HTTP::Request::Common;

ok( my $res = request('/foo/test'), "get /foo/test" );

#dump $res->headers;

is( $res->headers->{status}, 200, "get 200" );

ok( $res = request('/foo/1/read'), "get /foo/1/read" );

is( $res->headers->{status}, 200, "get 200" );

ok( $res = request('/foo/1/bars/2/add'),
    "GET /foo/1/bars/2/add" );

is( $res->headers->{status}, 400, "cannot GET add related" );

# add a new foobar
ok( $res = request( POST( '/foo/1/bars/2/add', [] ) ),
    "POST /foo/1/bars/2/add" );

is( $res->headers->{status}, 204, "POST add related OK" );

# remove an old foobar
ok( $res = request( POST( '/foo/1/bars/1/remove', [] ) ),
    "POST /foo/1/bars/1/remove" );

is( $res->headers->{status}, 204, "POST remove related OK" );

