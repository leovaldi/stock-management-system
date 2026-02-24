import pandas as pd
import numpy as np
import os
import re
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
    """Configura la conexión a BigQuery usando la llave local o el secreto de GitHub."""
    if os.path.exists("google_key.json"):
        return bigquery.Client.from_service_account_json("google_key.json")
    else:
        info = json.loads(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"))
        creds = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(credentials=creds, project=PROJECT_ID)

def descargar_datos():
    """Descarga y descomprime los archivos desde Kaggle."""
    print("Descargando archivos desde Kaggle...")
    api = KaggleApi()
    api.authenticate()
    if not os.path.exists('data'):
        os.makedirs('data')
    api.dataset_download_files(DATASET_SLUG, path='data/', unzip=True)
    print("Descarga y descompresion completada.")

def procesar_etl():
    # 1. Obtencion de datos
    descargar_datos()
    
    client = obtener_cliente_bq()
    credentials = client._credentials
    print("Iniciando proceso ETL...")

    # 2. Carga dinamica de archivos (basado en notebooks de limpieza)
    try:
        ventas_path = glob.glob('data/SalesFINAL*.csv')[0]
        compras_path = glob.glob('data/PurchasesFINAL*.csv')[0]
        det_compras_path = glob.glob('data/PurchasesFINAL*.csv')[0] # Usado para detalle
        inv_ini_path = glob.glob('data/BegInv*.csv')[0]
        inv_fin_path = glob.glob('data/EndInv*.csv')[0]
        
        ventas_raw = pd.read_csv(ventas_path)
        compras_raw = pd.read_csv(compras_path)
        inv_ini_raw = pd.read_csv(inv_ini_path)
        inv_fin_raw = pd.read_csv(inv_fin_path)
        
        print("Archivos cargados para procesamiento.")
    except IndexError:
        print("Error: Faltan archivos CSV en la carpeta data.")
        raise

    # --- TRANSFORMACIÓN: GENERAR CALENDARIO (Desde limpieza_ventas.ipynb) ---
    print("Generando Catalogo_Dim_Calendario desde ventas reales...")
    ventas_raw['SalesDate'] = pd.to_datetime(ventas_raw['SalesDate'])
    fechas_reales = ventas_raw['SalesDate'].unique()
    dim_calendario = pd.DataFrame({'Fecha': fechas_reales})
    dim_calendario['Fecha_ID'] = dim_calendario['Fecha'].dt.strftime('%Y%m%d').astype(int)
    dim_calendario['Año'] = dim_calendario['Fecha'].dt.year
    dim_calendario['Trimestre'] = dim_calendario['Fecha'].dt.quarter
    dim_calendario['Semana'] = dim_calendario['Fecha'].dt.isocalendar().week
    dim_calendario = dim_calendario.sort_values('Fecha').reset_index(drop=True)
    dim_calendario = dim_calendario[['Fecha_ID', 'Fecha', 'Año', 'Trimestre', 'Semana']]

    # --- TRANSFORMACIÓN: CATALOGO PRODUCTO Y TIENDA (Desde limpieza_productos.ipynb) ---
    print("Limpiando Dim_Producto y Dim_Tienda...")
    dim_producto = inv_ini_raw[['Brand', 'Description', 'Size', 'Price']].drop_duplicates()
    dim_producto.columns = ['Marca_ID', 'Descripcion', 'Tamano', 'Precio_Base']
    dim_producto['Descripcion'] = dim_producto['Descripcion'].str.strip() # Normalización de espacios
    dim_producto['Volumen'] = 0.0
    dim_producto['Clasificacion'] = 1.0
    dim_producto['Pack'] = "Individual"

    dim_tienda = inv_ini_raw[['Store', 'City']].drop_duplicates()
    dim_tienda.columns = ['Tienda_ID', 'Ciudad']

    # --- TRANSFORMACIÓN: PROVEEDORES Y COMPRAS (Desde limpieza_compras.ipynb) ---
    print("Procesando Compras y Proveedores...")
    dim_proveedor = compras_raw[['VendorNumber', 'VendorName']].drop_duplicates()
    dim_proveedor.columns = ['Proveedor_ID', 'Nombre_Proveedor']

    fact_compras = compras_raw.copy()
    fact_compras['Detalle_Compra_ID'] = fact_compras.index + 1
    fact_compras['Fecha_ID'] = pd.to_datetime(fact_compras['PODate']).dt.strftime('%Y%m%d').astype(int)
    fact_compras = fact_compras[['Detalle_Compra_ID', 'PONumber', 'VendorNumber', 'Brand', 'Fecha_ID', 'PurchasePrice', 'Quantity', 'Dollars']]
    fact_compras.columns = ['Detalle_Compra_ID', 'Compra_ID', 'Proveedor_ID', 'Marca_ID', 'Fecha_ID', 'Precio_Compra', 'Cantidad', 'Importe']

    # --- TRANSFORMACIÓN: VENTAS (Desde limpieza_ventas.ipynb) ---
    print("Procesando Fact_Ventas...")
    fact_ventas = ventas_raw.copy()
    fact_ventas['Venta_ID'] = fact_ventas.index + 1
    fact_ventas['Fecha_ID'] = fact_ventas['SalesDate'].dt.strftime('%Y%m%d').astype(int)
    fact_ventas['Cantidad'] = fact_ventas['SalesQuantity']
    fact_ventas['Venta_Total'] = fact_ventas['SalesDollars']
    fact_ventas['Precio_Unitario'] = fact_ventas['SalesPrice']
    fact_ventas['Impuesto'] = fact_ventas['ExciseTax']
    
    fact_ventas = fact_ventas[['Venta_ID', 'InventoryId', 'Brand', 'Store', 'Fecha_ID', 'Cantidad', 'Venta_Total', 'Precio_Unitario', 'Impuesto']]
    fact_ventas.columns = ['Venta_ID', 'Inventario_ID', 'Marca_ID', 'Tienda_ID', 'Fecha_ID', 'Cantidad', 'Venta_Total', 'Precio_Unitario', 'Impuesto']

    # --- TRANSFORMACIÓN: INVENTARIOS (Desde limpieza_inventario_inicial/final.ipynb) ---
    print("Normalizando Inventarios...")
    fact_inv_ini = inv_ini_raw[['InventoryId', 'Brand', 'Store', 'onHand']].copy()
    fact_inv_ini.columns = ['Inventario_ID', 'Marca_ID', 'Tienda_ID', 'Unidades_Disponibles']
    
    fact_inv_fin = inv_fin_raw[['InventoryId', 'Brand', 'Store', 'onHand']].copy()
    fact_inv_fin.columns = ['Inventario_ID', 'Marca_ID', 'Tienda_ID', 'Unidades_Disponibles']

    # --- CARGA A BIGQUERY ---
    tablas = {
        "Catalogo_Dim_Calendario": dim_calendario,
        "Catalogo_Dim_Producto": dim_producto,
        "Catalogo_Dim_Tienda": dim_tienda,
        "Catalogo_Dim_Proveedor": dim_proveedor,
        "Operaciones_Fact_Ventas": fact_ventas,
        "Operaciones_Fact_Compras": fact_compras,
        "Operaciones_Fact_Inventario_Inicial": fact_inv_ini,
        "Operaciones_Fact_Inventario_Final": fact_inv_fin
    }

    for nombre, df in tablas.items():
        print(f"Actualizando {nombre} en BigQuery...")
        df.to_gbq(
            f"{DATASET_ID}.{nombre}", 
            project_id=PROJECT_ID, 
            if_exists='replace',
            credentials=credentials
        )

    print("ETL Finalizado con éxito. Datos alineados con notebooks locales.")

if __name__ == "__main__":
    procesar_etl()