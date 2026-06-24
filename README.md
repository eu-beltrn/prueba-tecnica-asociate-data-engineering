Tu justificación de calidad y el diagrama conceptual de la arquitectura están **impecables**. Tienen un tono sumamente profesional, justifican técnicamente cada decisión de diseño y demuestran un dominio real de la infraestructura (mencionar el comportamiento de `psycopg2-binary` y SQLAlchemy suma muchos puntos).

Para que no dejes los bloques genéricos con corchetes (`[ ]`) en tu entrega final, integré tus textos directamente en la estructura del **`README.md`** definitivo. También corregí un pequeño salto de código que quedó cortado a la mitad en la sección de Git (`cd`).

Copia este bloque completo, pégalo en tu archivo **`README.md`** y tu documentación estará 100% lista para producción:

```markdown
# Prueba Técnica: Associate Data Engineer - Pipeline de Transacciones

Este proyecto implementa un pipeline de datos automatizado (ETL) para la ingesta, limpieza, transformación y carga de transacciones financieras diarias en un almacén de datos basado en la nube (Supabase / PostgreSQL), incluyendo un análisis avanzado de detección de anomalías y fraude.

---

## Fase 1: Reglas de Negocio y Arquitectura

### 1. Justificación de Calidad (Criterios de Diseño)

* **Regla 1 (Duplicados - id_transaccion):** Eliminar registros duplicados es crucial para evitar la sobreestimación del volumen transaccional y asegurar que cada evento financiero sea único.
* **Regla 2 (Tratamiento de Nulos - monto_usd):** Asignar `0.0` a las transacciones nulas que fueron "rechazadas" homologa el tipo de dato numérico sin alterar los balances financieros reales ni romper las operaciones matemáticas de la base de datos.
* **Regla 3 (Clasificación de Montos Inusuales - es_monto_inusual):** Precalcular esta bandera optimiza las consultas de la capa analítica al identificar inmediatamente transacciones internacionales críticas ($> \$1,500$ USD).
* **Regla 4 (Análisis de Anomalías):** Filtra estrictamente el universo de datos analizados para evaluar únicamente transacciones en estado "aprobada", ordenando el historial cronológicamente por cliente para rastrear picos abruptos de consumo mediante funciones de ventana.

### 2. Diagrama de Arquitectura Conceptual
El flujo sigue una arquitectura ETL limpia empleando formatos eficientes:

```text
[ transacciones_diarias.csv ]  <-- Fuente de Origen (Archivo Crudo)
             │
             ▼  (Ingesta e Inferencia de Tipos con Pandas)
      [ Capa Staging ]
             │
             ▼  (Lógica de Limpieza y Enriquecimiento) 
     [ Capa Enriquecida ]
             │
             ▼  (Carga en Bloques vía SQLAlchemy / Psycopg2) 
  [ Supabase (PostgreSQL) ]    <-- Base de Datos Centralizada (vía Puerto 6543)
             │
             ▼  (Filtrado y Ventanas de Tiempo con SQL / CTEs)
  [ Consulta de Anomalías ]    <-- Alertas para Prevención de Fraude

```

#### Librerías Utilizadas:

* **pandas:** Manipulación ágil de estructuras de datos bidimensionales en memoria.
* **sqlalchemy y psycopg2-binary:** Conectores robustos y seguros para interactuar dinámicamente con el motor de PostgreSQL en Supabase.

---

## Fase 2: Construcción (Python + Supabase)

### Requisitos Previos y Configuración

El proyecto fue desarrollado utilizando un entorno virtual de Python para garantizar el aislamiento de dependencias.

1. **Clonar el repositorio e ingresar a la carpeta:**
```bash
cd prueba-tecnica-asociate-data-engineering

```


2. **Crear y activar el entorno virtual (`venv`):**
```bash
# En Windows (PowerShell)
python -m venv venv
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
.\venv\Scripts\Activate.ps1

```


3. **Instalar dependencias requeridas:**
```bash
pip install -r requirements.txt

```



### Ejecución del Pipeline

Para iniciar el proceso de extracción, transformación y carga automática (ETL) hacia la base de datos, ejecuta:

```bash
python main.py

```

### Arquitectura de Conexión Usada (Estrategia de Infraestructura)

Durante el despliegue técnico, la conexión directa tradicional (puerto `5432`) experimentó bloqueos de resolución DNS residenciales. Para solucionar este problema de infraestructura de raíz, se implementó una conexión robusta hacia el **Transaction Pooler de Supabase (AWS West)** a través del puerto **`6543`**.

Esta arquitectura optimiza el pipeline mediante el uso de conexiones de corta duración y bajo consumo de memoria, ideal para ejecuciones breves e independientes como funciones serverless o scripts programados de automatización.

### Evidencia de Funcionamiento (Supabase)

A continuación se detalla el resultado de la consulta analítica ejecutada en el **SQL Editor de Supabase** para detectar picos de consumo sospechosos (monto actual $\ge$ 5x el monto anterior en transacciones aprobadas):

**Anomalías Detectadas en el Dataset:**

* **Cliente C-101:** Registró un pico crítico donde su gasto se multiplicó por **8.00** y **7.00** en transacciones consecutivas el mismo día. Alerta roja clásica de posible fraude.
* **Cliente C-146:** Registró un incremento inusual de **6.67** veces su consumo habitual en su transacción `T-051`.
* **Cliente C-120:** Registró un incremento exacto de **6.00** veces su consumo anterior en la transacción `T-024`.

---

## Fase 3: Propuesta de Orquestación (Apache Airflow)

Para un entorno productivo real, el pipeline se ha preparado conceptualmente para ser gestionado por **Apache Airflow**, programado para ejecutarse de manera automática **todos los días a las 11:30 PM**.

### Estructura del DAG (`dags/dag_transacciones.py`)

* **Frecuencia (Schedule):** Configurado de forma nativa mediante la expresión Cron `30 23 * * *`.
* **Resiliencia:** Implementa políticas de reintento (`retries: 1`, `retry_delay: 5 min`) para tolerar fluctuaciones o latencias temporales en los servicios de red.
* **Garantía de Dependencias:** Utiliza el operador de flujo `task_transformar_y_cargar >> task_analisis_anomalias`, asegurando de forma estricta que la consulta analítica en la base de datos solo se ejecute si la carga y limpieza de datos en Python culminó con un estado de éxito (`SUCCESS`).

### Simulación del Flujo en Entorno Local

Aunque Airflow requiere entornos nativos Linux/POSIX para su servidor web y scheduler, el archivo incluye un bloque de control de pruebas local. Puedes simular el orden lógico del flujo, validar las rutas absolutas e iniciar el ETL integrado corriendo:

```bash
python dags/dag_transacciones.py

```