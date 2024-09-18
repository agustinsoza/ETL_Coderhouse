CREATE TABLE IF NOT EXISTS agustinsoza964_coderhouse.clima_misiones(
	ID INT NOT NULL,
	Departamento VARCHAR(100),
	Temperatura FLOAT,
	Sensacion_Termica FLOAT,
	Temperatura_Min FLOAT,
	Temperatura_Max FLOAT,
	Humedad VARCHAR(5),
	Velocidad_Viento VARCHAR(20),
	Clima VARCHAR(100),
	Descripcion VARCHAR(100),
	Ultima_Actualizacion VARCHAR(100) NOT NULL,
	CONSTRAINT PK_clima_misiones PRIMARY KEY (ID, Ultima_Actualizacion));

TRUNCATE TABLE agustinsoza964_coderhouse.clima_misiones;

CREATE TABLE IF NOT EXISTS agustinsoza964_coderhouse.localidades_misiones (
	Localidad VARCHAR(100),
	Latitud DECIMAL(20,15),
	Longitud DECIMAL(20,15)
);

INSERT INTO agustinsoza964_coderhouse.localidades_misiones (localidad, latitud, longitud)
VALUES 
	('Apóstoles', -27.9087, -55.7514),
	('Cainguás', -27.2059, -54.9795),
	('Candelaria', -27.3933, -55.7532),
	('Capital', -27.3671, -55.8935),
	('Concepción', -27.9812, -55.5209),
	('Eldorado', -26.4083, -54.6984),
	('General Manuel Belgrano', -26.2625, -53.6482),
	('Guaraní', -27.2970, -54.2014),
	('Iguazú', -25.6111, -54.5737),
	('Leandro N. Alem', -27.60174491, -55.32662090),
	('Libertador General San Martín', -26.8060, -55.0233),
	('Montecarlo', -26.5664, -54.7598),
	('Oberá', -27.4874, -55.1185),
	('San Ignacio', -27.2666, -55.5282),
	('San Javier', -27.884636001620983, -55.11477950472755),
	('25 de Mayo', -27.3749, -54.7458),
	('San Pedro', -26.6196, -54.1083);