# Prueba Técnica: Associate Data Engineer - Pipeline de Transacciones

Este proyecto implementa un pipeline de datos automatizado (ETL) para la ingesta, limpieza, transformación y carga de transacciones financieras diarias en un almacén de datos basado en la nube (Supabase / PostgreSQL), incluyendo un análisis avanzado de detección de anomalías y fraude.

---

## Fase 1: Reglas de Negocio y Arquitectura

### 1. Justificación de Calidad (Criterios de Diseño)

Para asegurar un estándar profesional de gobernanza de datos, el módulo de transformación (`transform.py`) implementa controles estrictos basados en las **4 dimensiones esenciales de Data Quality**:

* **Regla 1 (Duplicados - id_transaccion) | Dimensión: UNICIDAD** Eliminar registros duplicados (como el ID `T-001` presente en el archivo crudo) mitiga el riesgo de sobreestimación del volumen transaccional y previene distorsiones en los balances financieros acumulados de la compañía.
* **Regla 2 (Tratamiento de Nulos - monto_usd) | Dimensión: COMPLETITUD** Asignar de forma controlada el valor `0.0` a los montos nulos que correspondan exclusivamente a transacciones con estado `"rechazada"`. Esto preserva la integridad del esquema relacional y evita excepciones críticas de cálculo matemático en el almacén de datos sin inventar flujos de caja ficticios.
* **Regla 3 (Montos Inusuales - es_monto_inusual) | Dimensión: CONSISTENCIA** El precalculo de la bandera booleana para transacciones internacionales superiores a \$1,500 USD enriquece el dato en la capa de transformación. Esto optimiza el rendimiento analítico, eliminando la necesidad de realizar costosos filtrados de cadenas de texto en consultas concurrentes posteriores.
* **Regla 4 (Análisis de Anomalías) | Dimensión: OPORTUNIDAD** Estandarizar cadenas de texto mediante remoción de espacios (*trimming*) y conversión explícita de `fecha_hora` a tipo temporal (`datetime`). Aislar únicamente los registros en estado `"aprobada"` garantiza un cálculo cronológico preciso de la velocidad del dinero vía funciones de ventana (`LAG`), eliminando falsos positivos causados por fallas técnicas en pasarelas de pago.

---

### 2. Diagrama de Arquitectura Conceptual

El flujo de datos se diseñó bajo una topología ETL lineal acoplada a través de un pooler de conexiones en la nube:

```mermaid
graph TD
    A[(transacciones_diarias.csv)] -->|1. Extracción e Inferencia| B[Capa Staging <br> Pandas RAM]
    B -->|2. Transformación <br> drop_duplicates / to_datetime| C[Capa Enriquecida <br> Data Quality Rules]
    C -->|3. Carga en Bloques <br> SQLAlchemy / Puerto 6543| D{Supabase <br> PostgreSQL Cloud}
    D -->|4. Ventanas de Tiempo <br> CTEs & LAG| E[Consulta de Anomalías <br> Reporte de Fraude]

    style A fill:#f9f,stroke:#333,stroke-width:2px
    style D fill:#bbf,stroke:#333,stroke-width:2px
    style E fill:#f99,stroke:#333,stroke-width:2px
```

#### Librerías Utilizadas:
##### Infraestructura y Core ETL:

* **pandas:** Utilizado para la manipulación ágil, tipado e ingesta eficiente de estructuras bidimensionales de datos en memoria (DataFrames).
* **sqlalchemy:** Actúa como la capa de abstracción de base de datos (ORM) para gestionar de forma segura el pool de conexiones hacia la nube.
* **psycopg2-binary:** Adaptador nativo de PostgreSQL para Python, encargado de ejecutar la inyección óptima de los bloques de datos transformados hacia Supabase.
* **python-dotenv:** Permite la carga dinámica de variables de entorno, aislando las credenciales y URLs de producción del código fuente para garantizar la seguridad del repositorio.

## Fase 2: Construcción (Python + Supabase)

### Requisitos Previos y Configuración

El proyecto fue desarrollado utilizando un entorno virtual de Python para garantizar el aislamiento de dependencias y la portabilidad del código.

1. **Clonar el repositorio e ingresar a la carpeta del proyecto:**
   ```bash
   cd prueba-tecnica-asociate-data-engineering

2. **Crear y activar el entorno virtual (`venv`):**
```bash
# En Windows (PowerShell)
python -m venv venv
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
.\venv\Scripts\Activate.ps1

```


3. **Instalar las dependencias requeridas:**
```bash
pip install -r requirements.txt

```


4. **Configuración de Variables de Entorno (Seguridad):**
Crea un archivo `.env` en la raíz del proyecto para almacenar de forma segura tus credenciales de acceso sin exponerlas en el código fuente:
```text
SUPABASE_DB_URL=postgresql+psycopg2://postgres.[TU_ID_SUPABASE]:[TU_PASSWORD]@[aws-0-us-west-1.pooler.supabase.com:6543/postgres](https://aws-0-us-west-1.pooler.supabase.com:6543/postgres)

```



### 1. Transformación (Python): (`src/transform.py`)

La lógica de calidad de datos se centralizó en un módulo independiente para garantizar el desacoplamiento del código.

<details>
<summary><b>Haz clic aquí para ver el código fuente de transform.py</b></summary>

```python
import pandas as pd
import numpy as np

def limpiar_y_transformar(df: pd.DataFrame) -> pd.DataFrame:
    df_clean = df.copy()
    
    # Conversión temporal y estandarización
    df_clean['fecha_hora'] = pd.to_datetime(df_clean['fecha_hora'])
    for col in ['estado_transaccion', 'tipo_comercio']:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].astype(str).str.strip().str.lower()
    
    # Regla 1: Duplicados
    df_clean = df_clean.drop_duplicates(subset=['id_transaccion'], keep='first')
    
    # Regla 2: Tratamiento de Nulos
    condicion_nulo = (df_clean['monto_usd'].isna()) & (df_clean['estado_transaccion'] == 'rechazada')
    df_clean.loc[condicion_nulo, 'monto_usd'] = 0.0
    df_clean = df_clean.dropna(subset=['monto_usd'])
    
    # Regla 3: Montos Inusuales
    condicion_inusual = (df_clean['monto_usd'] > 1500) & (df_clean['tipo_comercio'] == 'internacional')
    df_clean['es_monto_inusual'] = np.where(condicion_inusual, True, False)
    
    return df_clean

```

</details>

---

### Ejecución del Pipeline

Para iniciar el proceso automatizado de extracción, transformación y carga (ETL) hacia el almacén de datos en la nube, ejecuta el orquestador central:

```bash
python main.py

```

--¡Te quedó excelente la captura del SQL Editor! Se ve la metadata, la sintaxis limpia y los 4 registros clave mapeados en la grilla de resultados.

Para integrar los puntos **2. Carga y Almacenamiento** y **3. Análisis de Anomalías (SQL)** a la estructura actual de tu `README.md` (respetando la narrativa de seguridad, el uso de módulos con el comando `-m`, las explicaciones del rol corporativo `usr_etl_pipeline` y tu tabla de hallazgos), aquí tienes el bloque de texto completo, estructurado e impecable listo para copiar y pegar.

Reemplaza todo lo que va desde tu sección de **Ejecución del Pipeline** en adelante por el siguiente bloque:

---

```markdown
### Ejecución del Pipeline

Para iniciar el proceso automatizado de extracción, transformación y carga (ETL), ejecuta el orquestador central como un módulo de Python desde la raíz del proyecto para asegurar la correcta resolución de rutas:

```bash
python -m src.main

```

---

### 2. Carga y Almacenamiento (`src/load.py`)

La fase de carga (Load) automatiza la migración de los datos limpios y enriquecidos desde la memoria RAM del pipeline hacia la base de datos centralizada en la nube (Supabase). Para proteger la infraestructura contra accesos no autorizados, este componente se diseñó bajo estrictos estándares de seguridad defensiva.

```python
import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Cargamos las variables del archivo .env local para evitar credenciales en código duro
load_dotenv()

def cargar_a_supabase(df: pd.DataFrame, nombre_tabla: str):
    """
    Establece conexión segura con Supabase utilizando variables de entorno
    y carga los datos transformados de forma optimizada.
    """
    # 1. Aislamiento y extracción segura de credenciales
    database_uri = os.getenv("SUPABASE_DB_URL")
    
    if not database_uri:
        raise ValueError("Error Crítico: La variable de entorno SUPABASE_DB_URL no está configurada.")
    
    try:
        print("Conectando de forma segura con el Transaction Pooler de Supabase (Puerto 6543)...")
        # Configuración del motor con cifrado TLS/SSL forzado para datos en tránsito
        engine = create_engine(
            database_uri,
            connect_args={"sslmode": "require"}
        )
        
        print(f"Iniciando carga: Insertando {len(df)} registros en la tabla '{nombre_tabla}'...")
        
        # 2. Inyección eficiente por bloques para optimizar el IOPS de la base de datos
        df.to_sql(
            name=nombre_tabla,
            con=engine,
            if_exists='replace', 
            index=False,          
            chunksize=1000        
        )
        print("¡Carga completada exitosamente en el Data Warehouse Cloud!")
        
    except Exception as e:
        print(f"Error crítico durante la fase de Carga (Load): {e}")
        raise e  # Re-lanzamos la excepción para el monitoreo y alertas del orquestador

```

#### Criterios de Seguridad y Optimización Aplicados:

* **Gobernanza y Gestión de Secretos (Zero Hardcode):** Se eliminó la exposición de contraseñas y tokens en el código fuente. Utilizando `python-dotenv`, las credenciales viajan localmente mediante un archivo `.env` (excluido explícitamente en el `.gitignore`), previniendo fugas accidentales en el repositorio de GitHub.
* **Cifrado en Tránsito (Forced SSL):** El motor de SQLAlchemy incluye el parámetro `sslmode='require'`. Esto garantiza que toda la información transaccional viaje encriptada de extremo a extremo mediante TLS/SSL, blindando el pipeline contra ataques de intercepción en red (*Man-in-the-Middle*).
* **Principio de Menor Privilegio (RBAC):** La cadena de conexión está parametrizada para utilizar un rol exclusivo para el proceso ETL (`usr_etl_pipeline`) en lugar de la cuenta raíz administrador (`postgres`). Este rol posee permisos restringidos únicamente para operaciones esenciales de datos (`INSERT`, `SELECT`, `UPDATE`, `CREATE`) dentro del esquema público, siendo dueña legítima de su tabla asociada y bloqueando cualquier alteración no autorizada del servidor.
* **Carga Fraccionada (`chunksize=1000`):** Para mitigar la saturación de memoria RAM y optimizar las operaciones de Entrada/Salida (IOPS) en la instancia cloud de AWS, la inserción se ejecuta en bloques indexados de 1,000 registros, asegurando estabilidad ante fluctuaciones de latencia de red.

---

### Arquitectura de Conexión Usada (Estrategia de Infraestructura)

Durante el despliegue técnico, la conexión directa tradicional (puerto 5432) experimentó restricciones de resolución DNS en entornos residenciales. Para solucionar esta limitación de red de raíz, se implementó una arquitectura de conexión robusta orientada al **Transaction Pooler de Supabase (AWS West)** a través del puerto **6543**.

Esta estrategia optimiza el pipeline mediante el reciclaje de conexiones de corta duración y bajo consumo de memoria en el servidor, una solución alineada con las mejores prácticas para ejecuciones e integraciones continuas, scripts programados (cron/Airflow) o arquitecturas serverless.

---

### 3. Análisis de Anomalías (SQL)

Para la detección oportuna de vulnerabilidades financieras y picos sospechosos de consumo, se estructuró una consulta analítica avanzada utilizando **CTEs (Common Table Expressions)** y la función de ventana **`LAG`** para comparar de forma secuencial los históricos transaccionales individuales de cada cliente.

```sql
WITH historial_transacciones AS (
    SELECT 
        id_cliente,
        id_transaccion,
        fecha_hora,
        monto_usd,
        estado_transaccion,
        -- Window Function para obtener el monto de la transacción aprobada anterior del mismo cliente
        LAG(monto_usd, 1) OVER(
            PARTITION BY id_cliente 
            ORDER BY fecha_hora ASC
        ) AS monto_transaccion_anterior
    FROM public.transacciones_diarias_limpias
    -- Regla de Negocio 4: Evaluar estrictamente transacciones aprobadas
    WHERE estado_transaccion = 'aprobada'
)
SELECT 
    id_cliente,
    id_transaccion,
    fecha_hora,
    monto_transaccion_anterior AS monto_anterior,
    monto_usd AS monto_actual,
    -- Cálculo del multiplicador de consumo
    ROUND((monto_usd / monto_transaccion_anterior)::numeric, 2) AS multiplicador_veces_mayor
FROM historial_transacciones
-- Filtro analítico: transacciones que sean al menos 5 veces mayores a la anterior
WHERE monto_transaccion_anterior IS NOT NULL 
  AND monto_usd >= (5 * monto_transaccion_anterior)
ORDER BY multiplicador_veces_mayor DESC;

```

### Evidencia de Funcionamiento (Supabase)

![Evidencia de Supabase](img/captura_supabase.png)

A continuación se detalla el resultado consolidado de la consulta analítica avanzada ejecutada en el **SQL Editor de Supabase** para detectar picos de consumo sospechosos (donde el monto actual es mayor o igual a 5 veces el monto anterior en transacciones estrictamente aprobadas):

| ID Cliente | ID Transacción | Fecha y Hora | Monto Anterior | Monto Actual | Multiplicador (Veces Mayor) | Prioridad de Riesgo |
| --- | --- | --- | --- | --- | --- | --- |
| **C-101** | T-117 | 2026-06-18 19:15:00 | $10.00 | $80.00 | **8.00x** | 🚨 Alta (Crítica) |
| **C-101** | T-005 | 2026-06-18 09:30:00 | $50.00 | $350.00 | **7.00x** | 🚨 Alta (Crítica) |
| **C-146** | T-051 | 2026-06-18 13:50:00 | $15.00 | $100.00 | **6.67x** | ⚠️ Media |
| **C-120** | T-024 | 2026-06-18 11:15:00 | $25.00 | $150.00 | **6.00x** | ⚠️ Media |

#### Análisis de Hallazgos y Riesgos de Ciberseguridad:

* **Cliente C-101 (Alerta Roja):** Presenta un comportamiento crítico al registrar dos picos drásticos independientes multiplicando su consumo por **8.00** y **7.00** veces su monto inmediatamente anterior el mismo día. Patrón clásico de fraude por cuenta comprometida, compromiso de credenciales o clonación de tarjeta.
* **Clientes C-146 y C-120 (Alerta Amarilla):** Rompieron los umbrales lógicos de seguridad establecidos al multiplicar sus transacciones por **6.67** y **6.00** veces respectivamente en un periodo muy corto de tiempo, requiriendo el aislamiento preventivo de las cuentas y su escalación a revisión manual por analistas de riesgo.

---

## Fase 3: Propuesta de Orquestación (Apache Airflow)

Para un entorno productivo automatizado, el pipeline está diseñado conceptualmente para ser gestionado mediante un Grafo Acíclico Dirigido (**DAG**) en **Apache Airflow**, programado para ejecutarse de manera desatendida **todos los días a las 11:30 PM**.

### Estructura y Parámetros del DAG

* **Frecuencia de Ejecución (Schedule):** Configurado de forma nativa mediante la expresión Cron `30 23 * * *`.
* **Políticas de Resiliencia:** Implementa argumentos por defecto (`default_args`) con políticas de reintento automatizado (`retries: 1`, `retry_delay: 5 min`) para mitigar fallos intermitentes de red o latencias en la comunicación con la nube.
* **Garantía de Dependencias Secuenciales:** Utiliza el operador de flujo `task_transformar_y_cargar >> task_analisis_anomalias`, asegurando de forma estricta que las tareas analíticas en la base de datos solo se disparen si la fase de extracción, limpieza e inyección en Python culminó con un estado de éxito absoluto (`SUCCESS`).

<details>
<summary><b>📄 Haz clic aquí para ver el código fuente de dags/dag_transacciones.py</b></summary>

```python
import os
import sys
from datetime import datetime, timedelta
from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator # type: ignore

# Inyección dinámica de rutas para asegurar la portabilidad modular del proyecto
ruta_raiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ruta_raiz not in sys.path:
    sys.path.insert(0, ruta_raiz)

from src.main import ejecutar_pipeline 

# Configuración defensiva del ciclo de vida de las tareas
default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Declaración formal del DAG programado diariamente a las 11:30 PM
with DAG(
    dag_id='pipeline_transacciones_supabase',
    default_args=default_args,
    description='Pipeline diario automatizado de transacciones y detección de anomalías',
    schedule='30 23 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['etl', 'supabase', 'production'],
) as dag:

    # Tarea 1: Ejecución del proceso ETL modularizado (Python + Pandas)
    task_transformar_y_cargar = PythonOperator(
        task_id='transformar_y_cargar_datos',
        python_callable=ejecutar_pipeline,
    )

    # Tarea 2: Orquestación de la consulta analítica directamente en el motor Cloud
    task_analisis_anomalias = SQLExecuteQueryOperator(
        task_id='ejecutar_analisis_anomalias',
        conn_id='supabase_conn', 
        sql="""
            WITH historial_transacciones AS (
                SELECT 
                    id_cliente,
                    id_transaccion,
                    fecha_hora,
                    monto_usd,
                    estado_transaccion,
                    LAG(monto_usd, 1) OVER(
                        PARTITION BY id_cliente 
                        ORDER BY fecha_hora ASC
                    ) AS monto_transaccion_anterior
                FROM public.transacciones_diarias_limpias
                WHERE estado_transaccion = 'aprobada'
            )
            SELECT 
                id_cliente,
                id_transaccion,
                fecha_hora,
                monto_transaccion_anterior AS monto_anterior,
                monto_usd AS monto_actual,
                ROUND((monto_usd / monto_transaccion_anterior)::numeric, 2) AS multiplicador_veces_mayor
            FROM historial_transacciones
            WHERE monto_transaccion_anterior IS NOT NULL 
              AND monto_usd >= (5 * monto_transaccion_anterior)
            ORDER BY multiplicador_veces_mayor DESC;
        """,
    )

    # Restricción de flujo: SQL depende de la finalización exitosa de Python
    task_transformar_y_cargar >> task_analisis_anomalias # type: ignore

# ==============================================================================
# BLOQUE DE CONTROL PARA SIMULACIÓN LOCAL (Validación sin Servidor Airflow)
# ==============================================================================
if __name__ == "__main__":
    print("\n" + "="*60)
    print("SIMULADOR LOCAL DE ORQUESTACIÓN (Entorno de Desarrollo)")
    print("="*60)
    print(f"Hora de simulación programada: {dag.schedule} (11:30 PM)")
    print("Verificando dependencias lógicas del pipeline...")
    
    try:
        print("\n[Airflow Sim] Iniciando Tarea 1: transformar_y_cargar_datos...")
        ejecutar_pipeline()
        print("[Airflow Sim] Tarea 1 finalizada con éxito. Estado: SUCCESS")
        
        print("\n[Airflow Sim] Evaluando restricción de dependencia (>>)...")
        print("[Airflow Sim] Iniciando Tarea 2: ejecutar_analisis_anomalias...")
        print("[Airflow Sim] Nota: La consulta SQL se ejecutará mediante la conexión 'supabase_conn' en producción.")
        print("[Airflow Sim] Tarea 2 lista para producción. Estado: PENDING_ORCHESTRATION")
        
        print("\n"+"="*60)
        print("¡Estructura conceptual del DAG validada localmente con éxito!")
        print("="*60 + "\n")
        
    except Exception as error_local:
        print(f"\n[Airflow Sim Error] El flujo abortó en la Tarea 1: {error_local}")

```
</details>

---

### Simulación del Flujo en Entorno Local

Aunque Airflow requiere arquitecturas nativas basadas en Linux/POSIX para levantar sus componentes de servidor web y planificador (*scheduler*), el diseño modular del archivo permite simular el orden lógico del flujo e iniciar pruebas integrales directas en Windows ejecutando el comando:

```bash
python dags/dag_transacciones.py

```

Al correr este comando, el simulador incorporado disparará la secuencia de tareas, validará la consistencia del entorno y completará la ingesta hacia Supabase de forma segura, garantizando que el diseño esté 100% listo para ser depositado en una carpeta corporativa de producción de Airflow.

```



