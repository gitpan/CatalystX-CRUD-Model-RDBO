use Test::More tests => 5;

BEGIN {
    use lib qw( ../CatalystX-CRUD/lib );
    use_ok('CatalystX::CRUD::Model::RDBO');
    use_ok('CatalystX::CRUD::Object::RDBO');
    use_ok('Rose::DBx::TestDB');
    use_ok('Rose::DB::Object');
}

use lib qw( t/lib );
use Catalyst::Test 'MyApp';
use Data::Dump qw( dump );

ok( get('/foo'), "get /foo" );

