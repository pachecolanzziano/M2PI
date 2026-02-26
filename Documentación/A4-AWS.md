# FleetLogix: Plan de Modernización Cloud (AWS)

Este documento detalla la arquitectura de nube diseñada para **FleetLogix**, enfocada en la transición de una operación local a una infraestructura **Serverless** escalable, eficiente y capaz de procesar datos de flota en tiempo real.

---

## 1. Arquitectura en la Nube: Ingesta y Recepción

El objetivo es permitir que miles de conductores envíen datos de telemetría y entregas de forma simultánea sin degradar el rendimiento.

* **Amazon API Gateway:** Actúa como el punto de enlace único (REST API) para las aplicaciones móviles. Proporciona seguridad, control de cuotas y validación de datos inicial.
* **Amazon S3 (Data Lake):** Almacenamiento persistente de todo el historial de datos. Los archivos se organizan por particiones de fecha (`YYYY/MM/DD/`) para optimizar costos de almacenamiento y velocidad de consulta.
* **AWS Lambda (Ingesta):** Procesa cada "trigger" proveniente del API Gateway, separando la información para su almacenamiento histórico y su actualización en tiempo real.

---

## 2. Procesamiento Automatizado (Lógica Serverless)

Se han diseñado tres funciones **AWS Lambda** (Python + Boto3) para automatizar la toma de decisiones basada en datos:

### A. Verificación de Entrega
* **Acción:** Se activa al recibir un estado de "Completado".
* **Lógica:** Actualiza el registro de la entrega en la base de datos y marca el camión como "Disponible" para la siguiente ruta.

### B. Cálculo de ETA (Tiempo Estimado de Llegada)
* **Acción:** Se dispara con cada actualización de posición GPS.
* **Lógica:** Calcula la distancia entre las coordenadas actuales y el punto de destino definido en la hoja de ruta, proyectando el tiempo restante.

### C. Sistema de Alertas por Desvío
* **Acción:** Compara la posición del GPS contra la ruta planificada guardada en el sistema.
* **Lógica:** Si el vehículo se aleja del margen de tolerancia (Geofencing), envía una notificación automática vía **Amazon SNS** al supervisor de la flota.

---

## 3. Estrategia de Datos y Persistencia

Para maximizar la agilidad de la empresa, se implementa una estrategia de almacenamiento híbrida:

### Migración Relacional (AWS RDS)
* **Servicio:** AWS RDS para PostgreSQL.
* **Datos:** Información estructurada (Conductores, rutas planeadas, clientes, inventarios).
* **Resiliencia:** Configuración de **backups automáticos** y snapshots diarios para garantizar la recuperación ante desastres.

### Estado en Tiempo Real (Amazon DynamoDB)
* **Servicio:** DynamoDB (NoSQL).
* **Datos:** Estado actual de la flota (Última posición, estado de carga, batería).
* **Ventaja:** Escala masivamente con tiempos de respuesta de milisegundos, ideal para el monitoreo en vivo.

---

## Beneficios del Modelo
1.  **Cero Mantenimiento de Servidores:** Al ser Serverless, FleetLogix no gestiona parches ni hardware físico.
2.  **Pago por Uso:** Los costos se ajustan al tamaño de la flota y la actividad real.
3.  **Seguridad:** Integración nativa con AWS IAM para el control de acceso a los datos de la empresa.

---
![alt text](<grafico aws.png>)

## Diagrama de Flujo de Datos

El sistema sigue un modelo dirigido por eventos (Event-Driven Architecture), donde cada acción del conductor desencadena una respuesta automática en la nube.
---

## Descripción de los Procesos 

### 1. Ingesta de Datos (Entrada)
* **Origen:** La aplicación móvil del conductor envía un paquete de datos (JSON) con su GPS y estado.
* **Punto de Entrada (API Gateway):** Recibe la señal HTTPS. Su función es validar que el conductor esté autenticado y dirigir la información hacia el "cerebro" del sistema (Lambda).
* **Almacenamiento Crudo (Amazon S3):** Antes de cualquier proceso, una copia del dato original se guarda en S3 organizado por `/año/mes/día/`. Esto permite tener un **Data Lake** histórico para auditorías o análisis futuro de Big Data.

### 2. Procesamiento Inteligente 
Se ejecutan tres funciones **AWS Lambda** de forma paralela o secuencial según el evento:

* **Verificador de Entrega:** Si el sensor o el conductor marcan "Entregado", esta función actualiza el inventario y libera el vehículo para la siguiente carga.
* **Calculador de ETA:** Toma las coordenadas actuales y, consultando la ruta en la base de datos, calcula los minutos restantes para el destino.
* **Monitor de Desvíos:** Compara la posición GPS contra la "Geocerca" o ruta permitida. Si hay una anomalía, dispara una alerta inmediata.

### 3. Persistencia y Consultas
El sistema decide dónde guardar la información procesada según su utilidad:

* **Amazon DynamoDB (Estado Actual):** Guarda la "foto del momento". Es donde el panel de control de FleetLogix consulta para saber dónde está cada camión *ahora mismo*.
* **Amazon RDS (PostgreSQL):** Aquí reside la inteligencia del negocio (nombres de conductores, rutas asignadas, clientes). Se migraron los datos locales a este servicio administrado para contar con **Backups Automáticos** y alta disponibilidad.
* **Amazon SNS (Alertas):** En caso de desvíos o retrasos críticos, Lambda activa este servicio para enviar notificaciones Push o SMS a los supervisores.

---

## Tecnologías Clave Utilizadas

| Servicio | Rol en FleetLogix | Beneficio Principal |
| :--- | :--- | :--- |
| **AWS Lambda** | Procesamiento de lógica | Solo pagas por milisegundo de ejecución. |
| **Amazon S3** | Histórico de flota | Almacenamiento de bajo costo y alta durabilidad. |
| **boto3 (Python)** | SDK de comunicación | Permite que las funciones Lambda hablen con S3, RDS y DynamoDB. |
| **AWS RDS** | Base de datos relacional | Eliminamos la gestión de servidores físicos y parches de seguridad. |

---

## Seguridad y Resiliencia
* **Backups:** RDS realiza copias de seguridad automáticas diarias.
* **Escalabilidad:** Al ser una arquitectura serverless, si la flota crece de 10 a 1,000 camiones, AWS escala los recursos automáticamente sin intervención manual.

