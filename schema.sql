
database: my_database;

Table: users {
    id => pk.uuid (string) | pk.serial (int),
    name => text unique not null,
    email => unique,
    price => integer
}

Table: products {
    id => pk.uuid (string) | pk.serial (int),
    name => text unique not null,
    email => unique,
    price => integer
}

Table: sell {
    id => pk.serial (int),
    name => text unique not null,
    product_id text,
    user_id text,
    email => unique,
    price => integer

    users.id(user_id)
    products(product_id)
}