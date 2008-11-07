package MyApp::Model::Foo;
use base qw( CatalystX::CRUD::Model::RDBO );
__PACKAGE__->config(
    object_class => 'MyApp::Object',
    name         => My::Foo,
    load_with    => [qw( bar bars )]
);

1;
