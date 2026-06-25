import pandas as pd
import numpy as np

def limpiar_y_transformar(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica las reglas lógicas de negocio y estandarización para asegurar la calidad de los datos.
    """
    # Evitamos SettingWithCopyWarning operando sobre una copia explícita
    df_clean = df.copy()
    
    # Estandarización de Texto y Fechas (Pre-procesamiento crítico)
    df_clean['fecha_hora'] = pd.to_datetime(df_clean['fecha_hora'])
    
    for col in ['estado_transaccion', 'tipo_comercio']:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].astype(str).str.strip().str.lower()
    
    # Duplicación de Registros: Conservamos la primera ocurrencia basándonos en id_transaccion
    df_clean = df_clean.drop_duplicates(subset=['id_transaccion'], keep='first')
    
    # Tratamiento de Valores Faltantes: monto_usd nulo y rechazada -> 0.0
    condicion_nulo = (df_clean['monto_usd'].isna()) & (df_clean['estado_transaccion'] == 'rechazada')
    df_clean.loc[condicion_nulo, 'monto_usd'] = 0.0
    
    # Manejo de resguardo: Si hay nulos en otros estados, los eliminamos para proteger la BD
    df_clean = df_clean.dropna(subset=['monto_usd'])
    
    # Clasificación de Montos Inusuales: > 1500 USD e internacional
    condicion_inusual = (df_clean['monto_usd'] > 1500) & (df_clean['tipo_comercio'] == 'internacional')
    df_clean['es_monto_inusual'] = np.where(condicion_inusual, True, False)
    
    return df_clean