import pandas as pd
import numpy as np
import os
import re
from google.cloud import bigquery
from kaggle.api.kaggle_api_extended import KaggleApi

# CONFIGURACIÓN
PROJECT_ID = "henry-inventory-analytics"  # <--- CAMBIA ESTO POR TU ID DE GOOGLE
DATASET = "Inventario_DWH"

def transformar_datos():
    print("Iniciando transformación de datos...")
    
    # --- EJEMPLO DE LÓGICA DE TUS NOTEBOOKS ---
    # Nota: Aquí asumo que los archivos están en una carpeta 'data'
    
    # 1. Dim_Producto (Catalogo)
    df_prod = pd.read_csv('data/BegInvFinal12312016.csv')  # Ejemplo de origen
    dim_producto = df_prod[['Brand', 'Description', 'Size', 'PurchasePrice', 'Volume', 'Classification']].drop_duplicates()
    
    # 2. Fact_Ventas (Operaciones) + Tu lógica de Packs
    df_ventas = pd.read_csv('data/SalesFINAL12312016.csv')
    df_ventas['SalesDate'] = pd.to_datetime(df_ventas['SalesDate'])
    
    # Tu función de limpieza de Packs (Regex)
    def calc_pack(val):
        res = re.findall(r'(\d+)\s*Pk', str(val))
        return int(res[0]) if res else 1
    
    df_ventas['Pack'] = df_ventas['Size'].apply(calc_pack)
    
    return dim_producto, df_ventas

def cargar_a_bigquery(df, tabla_nombre):
    print(f"Cargando {tabla_nombre}...")
    # Usamos el prefijo de tus esquemas
    tabla_destino = f"{DATASET}.{tabla_nombre}"
    df.to_gbq(tabla_destino, project_id=PROJECT_ID, if_exists='replace')

if __name__ == "__main__":
    # Aquí se dispararía la descarga de Kaggle y luego:
    d_prod, f_ventas = transformar_datos()
    
    cargar_a_bigquery(d_prod, "Catalogo_Dim_Producto")
    cargar_a_bigquery(f_ventas, "Operaciones_Fact_Ventas")
    
    print("Proceso completado exitosamente.")