import pandas as pd
from src.transform import limpiar_y_transformar
from src.load import cargar_a_supabase

def ejecutar_pipeline():
    print("Iniciando Pipeline de Transacciones...")
    
    # Extracción
    df_crudo = pd.read_csv('data/transacciones_diarias.csv')
    
    # Transformación
    df_procesado = limpiar_y_transformar(df_crudo)
    
    # Carga
    cargar_a_supabase(df_procesado, 'transacciones_diarias_limpias')
    
    print("Pipeline finalizado con éxito.")

if __name__ == "__main__":
    ejecutar_pipeline()