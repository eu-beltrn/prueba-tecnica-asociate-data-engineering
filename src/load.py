# src/load.py
from sqlalchemy import create_engine
import pandas as pd
import urllib.parse

def cargar_a_supabase(df: pd.DataFrame, nombre_tabla: str):
    """
    Establece conexión con Supabase utilizando los datos reales del 
    Transaction Pooler (Puerto 6543) obtenidos del panel oficial.
    """
    # 1. Contraseña real protegida para URL
    contrasena_cruda = "4NiJE@/k!y9%VXb"
    contrasena_segura = urllib.parse.quote_plus(contrasena_cruda)
    
    # 2. LA CADENA DE CONEXIÓN EXACTA DEL POOLER:
    # Formato obligatorio: postgres.ID_PROYECTO:PASSWORD@HOST_DE_TU_IMAGEN:6543/postgres
    DATABASE_URI = f"postgresql://postgres.krwigrqjeetneiyiesbn:{contrasena_segura}@aws-1-us-west-2.pooler.supabase.com:6543/postgres"
    
    try:
        print("Conectando con el Transaction Pooler de Supabase (AWS West)...")
        engine = create_engine(DATABASE_URI)
        
        print(f"Insertando {len(df)} registros en la tabla '{nombre_tabla}'...")
        df.to_sql(
            name=nombre_tabla,
            con=engine,
            if_exists='replace', 
            index=False,          
            chunksize=1000        
        )
        print("¡Carga completada exitosamente en Supabase!")
        
    except Exception as e:
        print(f"Error crítico durante la carga a la base de datos: {e}")