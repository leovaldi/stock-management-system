import pandas as pd
import numpy as np
import os
import re
import json
from google.oauth2 import service_account
from google.cloud import bigquery
from kaggle.api.kaggle_api_extended import KaggleApi

# CONFIGURACIÓN
PROJECT_ID = "henry-inventory-analytics"  # <--- CAMBIA ESTO POR TU ID DE GOOGLE
DATASET_ID = "Inventario_DWH"

def obtener_cliente_bq():
    """Configura la conexión a BigQuery usando la llave local o el secreto de GitHub."""
    if os.path.exists("google_key.json"):
        return bigquery.Client.from_service_account_json("google_key.json")
    else:
        # Para GitHub Actions
        info = json.loads(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"))
        creds = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(credentials=creds, project=PROJECT_ID)

def limpiar_packs(valor):
    """Tu lógica de Regex para calcular unidades reales."""
    res = re.findall(r'(\d+)\s*Pk', str(valor))
    return int(res[0]) if res else 1

def procesar_etl():
    client = obtener_cliente_bq()
    print("Iniciando proceso ETL...")

    # 1. CARGA DE DATOS (Asumiendo que los CSV están en carpeta 'data')
    ventas_raw = pd.read_csv('data/SalesFINAL12312016.csv')
    compras_raw = pd.read_csv('data/PurchasesFINAL12312016.csv')
    inv_ini_raw = pd.read_csv('data/BegInvFinal12312016.csv')
    inv_fin_raw = pd.read_csv('data/EndInvFinal12312016.csv')

    # --- TRANSFORMACIÓN: CATALOGO (Dimensiones) ---
    
    # Dim_Producto
    print("Creando Dim_Producto...")
    dim_producto = inv_ini_raw[['Brand', 'Description', 'Size', 'Price']].drop_duplicates()
    dim_producto.columns = ['Marca_ID', 'Descripcion', 'Tamano', 'Precio_Base']
    dim_producto['Volumen'] = 0.0
    dim_producto['Clasificacion'] = 1.0
    dim_producto['Pack'] = "Individual"

    # Dim_Tienda
    dim_tienda = inv_ini_raw[['Store', 'City']].drop_duplicates()
    dim_tienda.columns = ['Tienda_ID', 'Ciudad']

    # Dim_Proveedor
    dim_proveedor = compras_raw[['VendorNumber', 'VendorName']].drop_duplicates()
    dim_proveedor.columns = ['Proveedor_ID', 'Nombre_Proveedor']

    # --- TRANSFORMACIÓN: OPERACIONES (Hechos) ---

    # Fact_Ventas
    print("Creando Fact_Ventas...")
    fact_ventas = ventas_raw.copy()
    fact_ventas['Fecha_Venta'] = pd.to_datetime(fact_ventas['SalesDate'])
    fact_ventas['Cantidad'] = fact_ventas['SalesQuantity']
    fact_ventas['Venta_Total'] = fact_ventas['SalesDollars']
    fact_ventas['Precio_Unitario'] = fact_ventas['SalesPrice']
    fact_ventas['Impuesto'] = fact_ventas['ExciseTax']
    fact_ventas = fact_ventas[['InventoryId', 'Brand', 'Store', 'Fecha_Venta', 'Cantidad', 'Venta_Total', 'Precio_Unitario', 'Impuesto']]
    fact_ventas.rename(columns={'Brand': 'Marca_ID', 'Store': 'Tienda_ID'}, inplace=True)

    # Fact_Compras
    print("Creando Fact_Compras...")
    fact_compras = compras_raw.copy()
    fact_compras['Fecha_ID'] = pd.to_datetime(fact_compras['PODate']).dt.strftime('%Y%m%d').astype(int)
    fact_compras = fact_compras[['PurchasePrice', 'Quantity', 'Dollars', 'VendorNumber', 'Brand', 'PONumber']]
    fact_compras.columns = ['Precio_Compra', 'Cantidad', 'Importe', 'Proveedor_ID', 'Marca_ID', 'Compra_ID']

    # Fact_Inventarios
    fact_inv_ini = inv_ini_raw[['InventoryId', 'Brand', 'Store', 'onHand']].copy()
    fact_inv_ini.columns = ['Inventario_ID', 'Marca_ID', 'Tienda_ID', 'Unidades_Disponibles']
    
    fact_inv_fin = inv_fin_raw[['InventoryId', 'Brand', 'Store', 'onHand']].copy()
    fact_inv_fin.columns = ['Inventario_ID', 'Marca_ID', 'Tienda_ID', 'Unidades_Disponibles']

    # --- CARGA A BIGQUERY ---
    tablas = {
        "Catalogo_Dim_Producto": dim_producto,
        "Catalogo_Dim_Tienda": dim_tienda,
        "Catalogo_Dim_Proveedor": dim_proveedor,
        "Operaciones_Fact_Ventas": fact_ventas,
        "Operaciones_Fact_Compras": fact_compras,
        "Operaciones_Fact_Inventario_Inicial": fact_inv_ini,
        "Operaciones_Fact_Inventario_Final": fact_inv_fin
    }

    for nombre, df in tablas.items():
        print(f"Subiendo {nombre} a BigQuery...")
        df.to_gbq(f"{DATASET_ID}.{nombre}", project_id=PROJECT_ID, if_exists='replace')

    print("ETL Finalizado con éxito.")

if __name__ == "__main__":
    procesar_etl()