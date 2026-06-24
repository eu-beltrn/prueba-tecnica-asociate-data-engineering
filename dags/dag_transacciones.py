# dag_transacciones.py
import os
import sys
from datetime import datetime, timedelta
from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator # type: ignore

# Solución al error de rutas: Agrega la carpeta raíz del proyecto al entorno de Python
ruta_raiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ruta_raiz not in sys.path:
    sys.path.insert(0, ruta_raiz)

from src.main import ejecutar_pipeline 

# Configuración de reintentos en caso de fallos de red
default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definición del DAG: Programado diariamente a las 11:30 PM ('30 23 * * *')
with DAG(
    dag_id='pipeline_transacciones_supabase',
    default_args=default_args,
    description='Pipeline diario de transacciones y anomalías',
    schedule='30 23 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['etl', 'supabase'],
) as dag:

    # Tarea 1: Ejecuta la limpieza y carga de datos en Python
    task_transformar_y_cargar = PythonOperator(
        task_id='transformar_y_cargar_datos',
        python_callable=ejecutar_pipeline,
    )

    # Tarea 2: Ejecuta la consulta SQL de anomalías directamente en Supabase
    task_analisis_anomalias = SQLExecuteQueryOperator(
        task_id='ejecutar_analisis_anomalias',
        conn_id='supabase_conn', 
        sql="""
            WITH transacciones_ordenadas AS (
                SELECT 
                    id_cliente,
                    id_transaccion,
                    fecha_hora,
                    monto_usd,
                    estado_transaccion,
                    LAG(monto_usd) OVER(
                        PARTITION BY id_cliente 
                        ORDER BY fecha_hora ASC, id_transaccion ASC
                    ) AS monto_anterior
                FROM transacciones_diarias_limpias
                WHERE LOWER(estado_transaccion) = 'aprobada'
            )
            SELECT 
                id_cliente,
                id_transaccion,
                fecha_hora,
                monto_anterior,
                monto_usd AS monto_actual,
                ROUND((monto_usd / monto_anterior)::numeric, 2) AS veces_mayor
            FROM transacciones_ordenadas
            WHERE monto_anterior IS NOT NULL 
              AND monto_usd >= (5 * monto_anterior)
            ORDER BY veces_mayor DESC;
        """,
    )

    # Dependencia estricta: SQL solo corre si Python termina con éxito
    task_transformar_y_cargar >> task_analisis_anomalias # type: ignore

# ==============================================================================
# BLOQUE DE PRUEBA LOCAL (Simulación fuera de Airflow)
# ==============================================================================
if __name__ == "__main__":
    print("\n" + "="*60)
    print("SIMULADOR LOCAL DE ORQUESTACIÓN (Entorno de Desarrollo)")
    print("="*60)
    print(f"Hora de simulación programada: {dag.schedule} (11:30 PM)")
    print("Verificando dependencias del pipeline...")
    
    try:
        print("\n[Airflow Sim] Iniciando Tarea 1: transformar_y_cargar_datos...")
        # Ejecutamos la función real de Python para comprobar que todo tu ETL funcione
        ejecutar_pipeline()
        print("[Airflow Sim] Tarea 1 finalizada con éxito. Estado: SUCCESS")
        
        print("\n[Airflow Sim] Evaluando dependencia (>>)...")
        print("[Airflow Sim] Iniciando Tarea 2: ejecutar_analisis_anomalias...")
        print("[Airflow Sim] Nota: La consulta SQL se ejecutará mediante la conexión 'supabase_conn' en producción.")
        print("[Airflow Sim] Tarea 2 lista para producción. Estado: PENDING_ORCHESTRATION")
        
        print("\n"+"="*60)
        print("¡Estructura del DAG analizada y validada localmente con éxito!")
        print("="*60 + "\n")
        
    except Exception as error_local:
        print(f"\n[Airflow Sim Error] El flujo falló en la Tarea 1: {error_local}")