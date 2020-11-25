create database HT2_201408486

use HT2_201408486;

create table temporal(
	carne varchar(50),
	nombre varchar(200),
	posiblenota varchar(50)
);


create table Alumno(
	carne int primary key,
	nombre varchar(200),
	posiblenota int
);

select * from temporal;
select * from Alumno;


delete from temporal;
delete from Alumno;