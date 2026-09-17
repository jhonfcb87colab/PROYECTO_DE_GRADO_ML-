# Presentado por:
* John Fernando Cruz Becerra
* Oscar Javier Huertas Campo
* Mauricio Alejandro Gaviria Alzate



A continuación presento la adaptación y reestructuración formal de tu texto. Se ha ajustado la redacción, el enfoque y la terminología para transformar la propuesta —que originalmente correspondía a un trabajo de la asignatura de Big Data sobre ventas globales— en una propuesta formal de Proyecto de Tesis de Maestría en Ciencia de Datos, centrada explícitamente en el ámbito de logística de última milla, predicción de tiempos de entrega y anticipación de incumplimientos de SLA para Servienvíos en Bogotá.

PROPUESTA DE PROYECTO DE TESIS DE MAESTRÍA EN CIENCIA DE DATOS
TÍTULO:

Modelo de aprendizaje automático para la predicción de tiempos de entrega y la anticipación de incumplimientos de SLA en la logística de última milla de Servienvíos (Bogotá)

1. Introducción
La logística de última milla representa una de las etapas más críticas, costosas y complejas dentro de la cadena de suministro, impactando de forma directa la satisfacción del cliente y la eficiencia operativa de las empresas de transporte de mercancías. En un contexto urbano denso y dinámico como el de Bogotá D.C., factores como el tráfico vehicular, las condiciones meteorológicas, la dispersión geográfica de las entregas y las variaciones en la demanda operativa generan una alta variabilidad en los tiempos de tránsito. Esto dificulta el cumplimiento estricto de los Acuerdos de Nivel de Servicio (SLA, por sus siglas en inglés).

El presente proyecto de grado de la Maestría en Ciencia de Datos aborda esta problemática mediante el diseño e implementación de una arquitectura integral de Big Data e Ingeniería de Datos bajo el enfoque Medallion (Bronze, Silver y Gold) sobre el ecosistema Spark y Delta Lake, orientada al procesamiento, análisis y modelado analítico de los datos operativos de la logística de última milla de Servienvíos (Bogotá).

A través de esta infraestructura distribuida y escalable, se busca consolidar un flujo continuo de datos transaccionales, de rastreo y de georreferenciación para alimentar modelos predictivos de Machine Learning. Estos modelos tendrán la capacidad de estimar de manera precisa los tiempos de entrega (problema de regresión) y clasificar tempranamente las órdenes en riesgo de incumplimiento de SLA (problema de clasificación), permitiendo a la organización tomar acciones preventivas y optimizar la toma de decisiones estratégicas y operativas.

2. Planteamiento de Objetivos
2.1 Objetivo General
Diseñar e implementar un sistema predictivo soportado en una arquitectura de Big Data tipo Medallion (Bronze, Silver y Gold) sobre Apache Spark y Delta Lake, que integre modelos de Machine Learning para la predicción precisa de los tiempos de entrega y la detección temprana de posibles incumplimientos en los Acuerdos de Nivel de Servicio (SLA) en la operación de logística de última milla de Servienvíos en la ciudad de Bogotá.

2.2 Objetivos Específicos
Implementar un pipeline de datos escalable y gobernado: Diseñar la arquitectura de ingesta y procesamiento distribuido utilizando Apache Spark y Delta Lake en capas Medallion (Bronze, Silver y Gold), garantizando calidad, consistencia, trazabilidad e integridad referencial de los registros transaccionales y de geolocalización.

Consolidar y depurar la información operativa: Ejecutar procesos de ingesta, limpieza, validación y estandarización de los datos históricos de guías, rutas, direcciones y estados de envío en la capa Silver, eliminando inconsistencias, sesgos y valores nulos.

Construir una capa de ingeniería de características (Feature Engineering): Desarrollar variables derivadas en la capa Gold que capturen el comportamiento de las entregas, tales como métricas geográficas, densidad de entregas por zona/localidad de Bogotá, históricos de desempeño por mensajero/vehículo, franjas horarias de tráfico, volumen de paquetes y ventanas temporales de SLA.

Desarrollar y entrenar modelos de Machine Learning: Construir y optimizar algoritmos de aprendizaje automático (p. ej., XGBoost, Random Forest, Regresión Logística, entre otros) orientados tanto a la estimación continua del tiempo de entrega como a la clasificación binaria de riesgo de fallo de SLA.

Evaluar la precisión y confiabilidad de los modelos: Validar el desempeño predictivo mediante métricas estadísticas y de aprendizaje automático (RMSE, MAE, Accuracy, Precision, Recall, F1-Score y AUC-ROC) utilizando validación cruzada y particionamiento temporal adecuado.

Habilitar una capa analítica para la toma de decisiones: Estructurar las salidas analíticas de la capa Gold y los resultados del modelo predictivo para su integración con herramientas de Business Intelligence (BI) y tableros operativos en tiempo real, facilitando la intervención temprana del equipo de logística.

3. Descripción General de la Solución Arquitectural y Analítica
El proyecto propone el diseño integral de un pipeline de analítica avanzada e ingeniería de datos distribuida, estructurado de la siguiente manera:

[Fuentes Operativas / CSV / GPS]
             │
             ▼
┌─────────────────────────┐
│     CAPA BRONZE         │  <-- Ingesta cruda, Delta Lake, Metadatos de auditoría
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│     CAPA SILVER         │  <-- Limpieza, Validación, Georreferenciación, Normalización
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│     CAPA GOLD           │  <-- Feature Store Lógico, Métricas SLA, Agregaciones por Zona
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│   CAPA MACHINE LEARNING │  <-- Modelos de Regresión y Clasificación (Predicción de SLA)
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│  BI & CONSUMO OPERATIVO │  <-- Tableros de Monitoreo / Alertas Preventivas de Entrega
└─────────────────────────┘
Capa Bronze (Raw Data Store):

Inicia con la ingesta de los datos históricos y transaccionales de la operación de Servienvíos en Bogotá (archivos de despacho, rutas, estados de guías y coordenadas). Los datos se almacenan en tablas Delta Lake manteniendo su estructura original, adicionando metadatos de trazabilidad como RUN_DATE, RUN_TS, canal de origen y control de versiones del lote cargado.

Capa Silver (Cleansed & Standardized Data):

Se aplican transformaciones distribuidas con Apache Spark para corregir direcciones, validar formatos de fecha/hora, depurar duplicados y calcular duraciones entre estados (p. ej., Asignado, En Ruta, Entregado). Esta etapa consolida un dataset confiable con datos de transporte y geolocalización limpios dentro del entorno urbano de Bogotá.

Capa Gold (Analytics & Feature Store Lógico):

Se diseñan agregaciones de negocio e ingeniería de características orientadas al problema logístico. Se calculan variables como: tiempo promedio de atención por zona geográfica, histórico de puntualidad por repartidor, densidad horaria de entregas, distancia estimada y márgenes de tiempo respecto a la ventana de SLA prometida al cliente.

Capa de Aprendizaje Automático (Machine Learning):

Se construyen los conjuntos de entrenamiento y prueba respetando la línea temporal de la operación. Se implementan y comparan múltiples algoritmos (Regresión Logística, Random Forest, XGBoost y ensamble de árboles) para predecir:

Tiempo de entrega en minutos (Target continuo).

Probabilidad de incumplimiento del SLA (Target binario).

Capa de Evaluación y Consumo:

Los modelos se evalúan rigurosamente con métricas como MAE/RMSE para la regresión, y F1-Score/AUC-ROC para la clasificación. La capa Gold resultante sirve como motor analítico consumible por dashboards operativos y sistemas de alerta temprana para la supervisión logística.

4. Configuración Global del Pipeline (Pipeline_Config_Init)
4.1 Descripción
Este componente inicializa y parametriza el entorno de ejecución distribuido del pipeline de datos para la operación de Servienvíos Bogotá. Centraliza las constantes del sistema, rutas de almacenamiento en data lakes y nombres de las tablas Delta dentro de la arquitectura Medallion.

4.2 Objetivo
Parametrizar las variables globales del pipeline ETL/ELT para asegurar la reusabilidad, portabilidad entre entornos (Desarrollo, Pruebas, Producción) y la gobernanza del procesamiento de datos logísticos.

4.3 Entradas y Atributos Procesados
Fuentes: Archivos de operaciones logísticas, historiales de despachos y eventos de trazabilidad de última milla.

Atributos:

Identificador de guía / envío

Datos de origen y destino (Localidad / Coordenadas en Bogotá)

Vehículo / Tipo de transporte

Tiempos marcados (Timestamp de despacho, llegada y entrega)

Estado final y cumplimiento de ventana SLA

4.4 Procesamiento
Definición de variables de entorno:

CATALOG, SCHEMA, RUN_DATE, RUN_TS.

Configuración de tablas Delta Lake:

BRONZE_LOGISTICA_SERVIENVIOS, SILVER_LOGISTICA_SERVIENVIOS, GOLD_SLA_PREDICTIONS.

Asignación de volúmenes e inicialización de logs y métricas de auditoría.

4.5 Salidas
Diccionario de parámetros global en memoria.

Contexto de sesión de Spark configurado para ejecuciones incrementales y particionadas.

4.6 Consideración de Ingeniería de Datos
Se establece una estructura modular y gobernada mediante un esquema y catálogo dedicados. El uso de la variable RUN_DATE permite el particionamiento de las tablas Delta Lake por fecha de procesamiento, facilitando recargas incrementales, reprocesamiento de días específicos (backfilling) y auditorías de datos en la operación logística de la ciudad.

5. Conclusión General del Proyecto de Tesis
La implementación de una arquitectura de Big Data basada en Apache Spark y Delta Lake bajo la metodología Medallion proporciona una base sólida, escalable y gobernada para abordar la complejidad de la última milla en Bogotá. La combinación de técnicas avanzadas de ingeniería de datos con modelos predictivos de Machine Learning permite pasar de una gestión logística reactiva a una estrategia proactiva, anticipando los fallos de SLA antes de que ocurran y optimizando la asignación de recursos en la operación de Servienvíos.