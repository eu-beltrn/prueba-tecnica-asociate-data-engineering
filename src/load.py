import os
import urllib.parse
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Cargamos las variables del archivo .env local
load_dotenv()

def cargar_a_supabase(df: pd.DataFrame, nombre_tabla: str):
    """
    Establece conexión segura con Supabase utilizando variables de entorno
    y carga los datos transformados de forma optimizada.
    """
    # 1. Extraer credenciales aisladas del código de forma segura
    database_uri = os.getenv("SUPABASE_DB_URL")
    
    if not database_uri:
        raise ValueError("Error Crítico: La variable de entorno SUPABASE_DB_URL no está configurada.")
    
    try:
        print("Conectando de forma segura con el Transaction Pooler de Supabase (Puerto 6543)...")
        # Configurar el motor con SSL requerido para cifrar los datos en tránsito
        engine = create_engine(
            database_uri,
            connect_args={"sslmode": "require"}
        )
        
        print(f"Iniciando carga: Insertando {len(df)} registros en la tabla '{nombre_tabla}'...")
        
        # 2. Inyección eficiente por bloques
        df.to_sql(
            name=nombre_tabla,
            con=engine,
            if_exists='replace', 
            index=False,          
            chunksize=1000        
        )
        print("¡Carga completada exitosamente en el Data Warehouse Cloud (supabase)!")
        
    except Exception as e:
        print(f"Error crítico durante la fase de Carga (Load): {e}")
        raise e  # Re-lanzamos el error para que Airflow o el main lo capturen y emitan alertas