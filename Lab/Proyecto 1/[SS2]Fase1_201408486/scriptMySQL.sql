create database seminariodos201408486_MYSQL;

use seminariodos201408486_MYSQL;


CREATE TABLE TemporalCompras 
    (
     fecha VARCHAR (50) , 
     codProveedor VARCHAR (50) , 
     nombreProveedor VARCHAR (200) , 
     direccionProveedor VARCHAR (200) , 
     numeroProveedor CHAR (50) , 
     webProveedor VARCHAR (50) , 
     codProducto VARCHAR (50) , 
     nombreProducto VARCHAR (200) , 
     grupoProducto VARCHAR (50) , 
     tipoProducto VARCHAR (200) , 
     codSucursal VARCHAR (50) , 
     nombreSucursal VARCHAR (200) , 
     direccionSucursal VARCHAR (200) , 
     region VARCHAR (200) , 
     departamento VARCHAR (200) , 
     zona VARCHAR (200) , 
     unidades VARCHAR (200) , 
     costo VARCHAR (200) 
    );

CREATE TABLE TemporalVentas 
    (
     fecha VARCHAR (100) , 
     codigoCliente VARCHAR (100) , 
     nombreCliente VARCHAR (200) , 
     tipoCliente VARCHAR (200) , 
     direccionCliente VARCHAR (200) , 
     numeroCliente VARCHAR (100) , 
     codProducto VARCHAR (100) , 
     nombreProducto VARCHAR (200) , 
     grupoProducto VARCHAR (100) , 
     tipoProducto VARCHAR (100) , 
     codSucursal VARCHAR (100) , 
     nombreSucursal VARCHAR (200) , 
     direccionSucursal VARCHAR (200) , 
     region VARCHAR (100) , 
     departamento VARCHAR (100) , 
     zona VARCHAR (100) , 
     codVendedor VARCHAR (100) , 
     nombreVendedor VARCHAR (200) , 
     sucursal VARCHAR (200) , 
     unidades VARCHAR (100) , 
     precioUnitario VARCHAR (100) 
    );
    
    
select * from TemporalCompras;
select * from TemporalVentas;