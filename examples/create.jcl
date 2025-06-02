CREATE TABLE users (
  id SERIAL PRIMKEY, 
  name VARCHAR(50) NOT NULL UNIQUE,
  age INT CHECK(age > 0),
  email VARCHAR(100) DEFAULT "unknown", 
  role_id INT FRNKEY REFERENCES roles(id)
);

CREATE TABLE orders (
  id SERIAL PRIMKEY,
  email VARCHAR(255)
); 