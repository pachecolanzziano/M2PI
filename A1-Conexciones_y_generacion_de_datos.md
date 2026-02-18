# Generación de datos para FLEETLOGIX
## Conexión a la base de datos
Los datos de la conexcion se manejan bajo variables de entorno para con el archivo .env

## Actualización de distancias
Se cambiaron las distancias entre las rutas para hacer mas reales, segun los datos proporcionados por Google Maps

## Problema en la generación 
El error ocurría específicamente en la línea donde se asignaba el peso de cada paquete: 
`package_weight = weights[i]`
Este valor era de tipo numpy.float64, un formato que PostgreSQL no reconoce. La solución fue modificar esa línea para convertir explícitamente el valor con `float(weights[i])`, transformándolo a un número decimal estándar que la base de datos sí puede almacenar. Además, se completaron las consultas SQL que estaban como #TO DO# para obtener correctamente los datos de viajes, rutas y vehículos.

