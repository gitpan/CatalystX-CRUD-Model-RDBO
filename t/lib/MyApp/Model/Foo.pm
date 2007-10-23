package MyApp::Model::Foo;
use base qw( CatalystX::CRUD::Model::RDBO );
__PACKAGE__->config->{object_class} = 'MyApp::Object';
__PACKAGE__->config->{name}         = 'My::Foo';

1;
