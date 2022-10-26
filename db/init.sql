CREATE TABLE IF NOT EXISTS miembros(nombre VARCHAR(50) NOT NULL,apellido VARCHAR(50) NOT NULL,correo VARCHAR(50) NOT NULL,patente VARCHAR(50) NOT NULL,rut VARCHAR(50) NOT NULL PRIMARY KEY,premium INT NOT NULL DEFAULT 0, stock_inicial INT NOT NULL);
CREATE TABLE IF NOT EXISTS ventas (id SERIAL PRIMARY KEY,patente VARCHAR(50) NOT NULL,cliente VARCHAR(50) NOT NULL,cantidad INT NOT NULL,data_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,ubicacion VARCHAR(50) NOT NULL);