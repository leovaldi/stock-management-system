import pandas as pd
from sqlalchemy import create_engine
import os
import sys

# =================================================================
# 1. CONFIGURACION DEL ENTORNO
# =================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RUTA_INGESTA = os.path.abspath(os.path.join(BASE_DIR, "..", "data", "DatosIngesta"))

CONFIG_SQL = {
    'server': 'localhost\\SQLEXPRESS', 
    'database': 'Inventario_DWH',
    'driver': 'ODBC Driver 17 for SQL Server'
}

# =================================================================
# 2. MOTOR DE CONEXION
# =================================================================
def obtener_motor():
    try:
        conn_str = (
            f"mssql+pyodbc://@{CONFIG_SQL['server']}/{CONFIG_SQL['database']}"
            f"?driver={CONFIG_SQL['driver']}&trusted_connection=yes"
        )
        return create_engine(conn_str)
    except Exception as e:
        print(f"Error de conexion: {e}")
        sys.exit()

# =================================================================
# 3. LOGICA DE INGESTA Y LIMPIEZA
# =================================================================
def ejecutar_carga():
    engine = obtener_motor()
    
    # El orden es fundamental para respetar las llaves foraneas
    pipeline_carga = [
        ('Dim_Calendario.csv', 'Catalogo', 'Dim_Calendario'),
        ('Dim_Proveedor.csv', 'Catalogo', 'Dim_Proveedor'),
        ('Dim_Tienda.csv', 'Catalogo', 'Dim_Tienda'),
        ('Dim_Producto.csv', 'Catalogo', 'Dim_Producto'),
        ('Fact_Ventas.csv', 'Operaciones', 'Fact_Ventas'),
        ('Fact_Compras.csv', 'Operaciones', 'Fact_Compras'),
        ('Fact_Inventario_Inicial.csv', 'Operaciones', 'Fact_Inventario_Inicial'),
        ('Fact_Inventario.csv', 'Operaciones', 'Fact_Inventario_Final')
    ]

    print(f"--- INICIANDO INGESTA AUTOMATIZADA EN {CONFIG_SQL['database']} ---")
    
    # Diccionarios para validar integridad referencial en memoria
    maestros = {
        'Marca_ID': [],
        'Tienda_ID': [],
        'Proveedor_ID': [],
        'Fecha_ID': []
    }

    for archivo, esquema, tabla in pipeline_carga:
        ruta_archivo = os.path.join(RUTA_INGESTA, archivo)
        
        if os.path.exists(ruta_archivo):
            try:
                print(f"Procesando: {archivo} >> {esquema}.{tabla}")
                df = pd.read_csv(ruta_archivo)

                # Limpieza basica
                df = df.replace('Unknown', 0)
                df = df.fillna(0)

                # Evitar duplicados en llaves primarias de dimensiones
                if esquema == 'Catalogo':
                    col_pk = df.columns[0]
                    df = df.drop_duplicates(subset=[col_pk])

                # Recoleccion de llaves para validar hechos
                if tabla == 'Dim_Producto': maestros['Marca_ID'] = df['Marca_ID'].unique().tolist()
                if tabla == 'Dim_Tienda': maestros['Tienda_ID'] = df['Tienda_ID'].unique().tolist()
                if tabla == 'Dim_Proveedor': maestros['Proveedor_ID'] = df['Proveedor_ID'].unique().tolist()
                if tabla == 'Dim_Calendario': maestros['Fecha_ID'] = df['Fecha_ID'].unique().tolist()

                # Validacion de Integridad Referencial (evita errores de FK en SQL)
                if esquema == 'Operaciones':
                    if 'Marca_ID' in df.columns:
                        df = df[df['Marca_ID'].isin(maestros['Marca_ID'])]
                    if 'Tienda_ID' in df.columns:
                        df = df[df['Tienda_ID'].isin(maestros['Tienda_ID'])]
                    if 'Proveedor_ID' in df.columns:
                        df = df[df['Proveedor_ID'].isin(maestros['Proveedor_ID'])]
                    if 'Fecha_ID' in df.columns:
                        df = df[df['Fecha_ID'].isin(maestros['Fecha_ID'])]

                # Insercion en la base de datos
                # Se utiliza append ya que la estructura fue creada previamente por el script SQL
                df.to_sql(name=tabla, con=engine, schema=esquema, if_exists='append', index=False, chunksize=1000)
                print(f"   Exito: {len(df)} filas insertadas.")

            except Exception as e:
                print(f"   Error en tabla {tabla}: {e}")
        else:
            print(f"   Aviso: El archivo {archivo} no se encuentra en la ruta.")

    print("--- PROCESO FINALIZADO ---")

if __name__ == "__main__":
    ejecutar_carga()