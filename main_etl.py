import pandas as pd
import numpy as np
import os
import json
import glob
from google.oauth2 import service_account
from google.cloud import bigquery
from kaggle.api.kaggle_api_extended import KaggleApi

# CONFIGURACIÓN
PROJECT_ID = "henry-inventory-analytics"
DATASET_ID = "Inventario_DWH"
DATASET_SLUG = "bhanupratapbiswas/inventory-analysis-case-study"

def obtener_cliente_bq():
    if os.path.exists("google_key.json"):
        return bigquery.Client.from_service_account_json("google_key.json")
    else:
        info = json.loads(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"))
        creds = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(credentials=creds, project=PROJECT_ID)

def descargar_datos():
    api = KaggleApi()
    api.authenticate()
    if not os.path.exists('data'): os.makedirs('data')
    api.dataset_download_files(DATASET_SLUG, path='data/', unzip=True)

def procesar_etl():
    descargar_datos()
    client = obtener_cliente_bq()
    credentials = client._credentials
    
    # Carga de archivos crudos
    ventas_raw = pd.read_csv(glob.glob('data/SalesFINAL*.csv')[0])
    compras_raw = pd.read_csv(glob.glob('data/PurchasesFINAL*.csv')[0])
    inv_ini_raw = pd.read_csv(glob.glob('data/BegInv*.csv')[0])
    inv_fin_raw = pd.read_csv(glob.glob('data/EndInv*.csv')[0])

    # --- 1. DIMENSIONES (CATALOGO) ---
    
    # Dim_Calendario (Extraído de Ventas según notebook)
    fechas = pd.to_datetime(ventas_raw['SalesDate']).unique()
    dim_calendario = pd.DataFrame({'Fecha': fechas})
    dim_calendario['Fecha_ID'] = dim_calendario['Fecha'].dt.strftime('%Y%m%d').astype(int)
    dim_calendario['Año'] = dim_calendario['Fecha'].dt.year
    dim_calendario['Mes'] = dim_calendario['Fecha'].dt.month
    dim_calendario['Trimestre'] = dim_calendario['Fecha'].dt.quarter
    dim_calendario['Semana'] = dim_calendario['Fecha'].dt.isocalendar().week
    dim_calendario = dim_calendario[['Fecha_ID', 'Fecha', 'Año', 'Mes', 'Trimestre', 'Semana']].drop_duplicates()

    # Dim_Tienda
    dim_tienda = inv_ini_raw[['Store', 'City']].drop_duplicates()
    dim_tienda.columns = ['Tienda_ID', 'Ciudad']

    # Dim_Proveedor
    dim_proveedor = compras_raw[['VendorNumber', 'VendorName']].drop_duplicates()
    dim_proveedor.columns = ['Proveedor_ID', 'Nombre_Proveedor']

    # Dim_Producto
    dim_producto = inv_ini_raw[['Brand', 'Description', 'Size', 'Price']].drop_duplicates()
    dim_producto.columns = ['Marca_ID', 'Descripcion', 'Tamaño', 'Volumen'] # Volumen se inicializa
    dim_producto['Volumen'] = 0.0
    dim_producto['Clasificacion'] = 1.0
    dim_producto['Pack'] = "Individual"
    dim_producto = dim_producto[['Marca_ID', 'Descripcion', 'Tamaño', 'Volumen', 'Clasificacion', 'Pack']]

    # --- 2. HECHOS (OPERACIONES) ---

    # Fact_Ventas
    fact_ventas = ventas_raw.copy()
    fact_ventas['Venta_ID'] = fact_ventas.index + 1
    fact_ventas['Fecha_ID'] = pd.to_datetime(fact_ventas['SalesDate']).dt.strftime('%Y%m%d').astype(int)
    fact_ventas = fact_ventas[['Venta_ID', 'Brand', 'Store', 'Fecha_ID', 'SalesQuantity', 'SalesDollars', 'SalesPrice', 'ExciseTax']]
    fact_ventas.columns = ['Venta_ID', 'Marca_ID', 'Tienda_ID', 'Fecha_ID', 'Cantidad', 'Venta_Total', 'Precio_Unitario', 'Impuesto']

    # Fact_Compras
    fact_compras = compras_raw.copy()
    fact_compras['Detalle_Compra_ID'] = fact_compras.index + 1
    fact_compras['Fecha_ID'] = pd.to_datetime(fact_compras['PODate']).dt.strftime('%Y%m%d').astype(int)
    fact_compras = fact_compras[['Detalle_Compra_ID', 'PONumber', 'Brand', 'VendorNumber', 'Fecha_ID', 'Quantity', 'PurchasePrice', 'Dollars']]
    fact_compras.columns = ['Detalle_Compra_ID', 'Compra_ID', 'Marca_ID', 'Proveedor_ID', 'Fecha_ID', 'Cantidad', 'Precio_Compra', 'Importe']

    # Fact_Inventario_Inicial
    fact_inv_ini = inv_ini_raw.copy()
    fact_inv_ini['Inventario_ID'] = fact_inv_ini.index + 1
    fact_inv_ini['Fecha_ID'] = 20160101 # Fecha fija de inicio según notebook
    fact_inv_ini = fact_inv_ini[['Inventario_ID', 'Brand', 'Store', 'Fecha_ID', 'onHand']]
    fact_inv_ini.columns = ['Inventario_ID', 'Marca_ID', 'Tienda_ID', 'Fecha_ID', 'Unidades_Disponibles']

    # Fact_Inventario (Final)
    fact_inv_fin = inv_fin_raw.copy()
    fact_inv_fin['Inventario_ID'] = fact_inv_fin.index + 1
    fact_inv_fin['Fecha_ID'] = 20161231 # Fecha fija de cierre según notebook
    fact_inv_fin = fact_inv_fin[['Inventario_ID', 'Brand', 'Store', 'Fecha_ID', 'onHand']]
    fact_inv_fin.columns = ['Inventario_ID', 'Marca_ID', 'Tienda_ID', 'Fecha_ID', 'Unidades_Disponibles']

    # --- 3. LIMPIEZA Y VALIDACIÓN (Igual al script de automatización local) ---
    tablas = {
        'Catalogo.Dim_Calendario': dim_calendario,
        'Catalogo.Dim_Proveedor': dim_proveedor,
        'Catalogo.Dim_Tienda': dim_tienda,
        'Catalogo.Dim_Producto': dim_producto,
        'Operaciones.Fact_Ventas': fact_ventas,
        'Operaciones.Fact_Compras': fact_compras,
        'Operaciones.Fact_Inventario_Inicial': fact_inv_ini,
        'Operaciones.Fact_Inventario_Final': fact_inv_fin
    }

    for full_name, df in tablas.items():
        # Limpieza según automatización: 'Unknown' -> 0 y fillna(0)
        df = df.replace('Unknown', 0).fillna(0)
        
        # Subir a BigQuery (Esquema_Tabla)
        table_id = full_name.replace('.', '_')
        print(f"Subiendo {table_id} a BigQuery...")
        df.to_gbq(f"{DATASET_ID}.{table_id}", project_id=PROJECT_ID, if_exists='replace', credentials=credentials)

    print("--- PROCESO FINALIZADO ---")

if __name__ == "__main__":
    procesar_etl()