import pandas as pd
import numpy as np

def limpiar_y_transformar(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica las reglas lógicas de negocio para asegurar la calidad de los datos.
    """
    df_clean = df.copy()
    
    # 1. Duplicación de Registros: Eliminar duplicados usando id_transaccion 
    df_clean = df_clean.drop_duplicates(subset=['id_transaccion'], keep='first')
    
    # 2. Tratamiento de Valores Faltantes: monto_usd nulo y rechazada -> 0.0 
    condicion_nulo = (df_clean['monto_usd'].isna()) & (df_clean['estado_transaccion'].str.lower() == 'rechazada')
    df_clean.loc[condicion_nulo, 'monto_usd'] = 0.0
    
    # 3. Clasificación de Montos Inusuales: > 1500 USD y tipo_comercio internacional 
    condicion_inusual = (df_clean['monto_usd'] > 1500) & (df_clean['tipo_comercio'].str.lower() == 'internacional')
    df_clean['es_monto_inusual'] = np.where(condicion_inusual, True, False)
    
    return df_clean