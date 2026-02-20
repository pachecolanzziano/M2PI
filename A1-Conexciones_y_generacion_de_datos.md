# Generación de datos para FLEETLOGIX
## Conexión a la base de datos
Los datos de la conexcion se manejan bajo variables de entorno para con el archivo .env

## Actualización de distancias
Se cambiaron las distancias entre las rutas para hacer mas reales, segun los datos proporcionados por Google Maps

## Problema en la generación 
El error ocurría específicamente en la línea donde se asignaba el peso de cada paquete: 
`package_weight = weights[i]`
Este valor era de tipo numpy.float64, un formato que PostgreSQL no reconoce. La solución fue modificar esa línea para convertir explícitamente el valor con `float(weights[i])`, transformándolo a un número decimal estándar que la base de datos sí puede almacenar. Además, se completaron las consultas SQL que estaban como #TO DO# para obtener correctamente los datos de viajes, rutas y vehículos.

## Entregas_imposibles
PROBLEMA: Las entregas están ocurriendo ANTES de que el viaje comience. Esto es ilógico físicamente.

CAUSA: En el código, cuando se calcula:
`scheduled = departure + timedelta`
no se verifica que scheduled sea después de departure.

En el proceso de validación y corrección de los datos, identifiqué inconsistencias relacionadas con los tiempos de entrega y programación frente a los horarios reales de salida y llegada. Para solucionarlo, implementé validaciones lógicas en el código que garantizan coherencia temporal. Primero, corregí los casos donde la hora de entrega era anterior a la salida, ajustándola a 30 minutos después de la salida 
`if delivered < departure: delivered = departure + 30min`.
 También controlé los casos donde la entrega superaba la hora de llegada, estableciendo como límite cinco minutos antes de la llegada
 `if delivered > arrival: delivered = arrival - 5min`
 Además, detecté que el uso de `randint(-30,30)` permitía generar tiempos programados antes del horario previsto, por lo que lo modifiqué a `randint(0,30)` para evitar valores negativos. Finalmente, añadí una validación para asegurar que la hora programada nunca fuera anterior a la salida, ajustándola a 30 minutos posteriores en caso contrario
 `if scheduled < departure: scheduled = departure + 30min` 
 
 