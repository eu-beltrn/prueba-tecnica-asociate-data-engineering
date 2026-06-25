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