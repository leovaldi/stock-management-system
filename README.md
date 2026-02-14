
---

# Sistema de Gestión de Inventario - Bloque 1

Este repositorio contiene la infraestructura necesaria para crear, limpiar e ingestar la base de datos de inventario. El flujo de trabajo está diseñado para ser reproducible mediante scripts de SQL y Python.

## 1. Requisitos Previos

Antes de comenzar, asegúrate de tener instalado:

* **SQL Server** (se recomienda la versión Express).
* **SQL Server Management Studio (SSMS)**.
* **Python 3.x**.

### Librerías de Python necesarias

Para que el script de automatización funcione, debes instalar las siguientes librerías ejecutando este comando en tu terminal:

```bash
pip install pandas sqlalchemy pyodbc

```

* **Pandas**: Para la manipulación y limpieza de los datos.
* **SQLAlchemy**: Para la gestión de la conexión a la base de datos.
* **PyODBC**: Para permitir que Python se comunique con SQL Server.

---

## 2. Paso a Paso para la Configuración

Sigue este orden estricto para garantizar la integridad de los datos:

### Paso 1: Creación de la Estructura en SQL

1. Abre **SQL Server Management Studio (SSMS)**.
2. Abre y ejecuta el archivo `scripts/creacion_tablas.sql`.
* *Este script borrará cualquier versión previa, creará la base de datos `Inventario_DWH` y definirá los esquemas y tablas con sus respectivas llaves primarias y foráneas.*



### Paso 2: Preparación de los Datos

1. Asegúrate de que los archivos CSV resultantes de la limpieza (los que están en la carpeta `data/DatosIngesta/`) estén presentes en tu directorio local.

### Paso 3: Ejecución de la Automatización (Ingesta)

1. Abre el archivo `scripts/automatizacion_ingesta.py`.
2. Busca la sección `CONFIG_SQL` y asegúrate de que el nombre del servidor coincida con el tuyo (normalmente es `localhost\SQLEXPRESS`).
3. Ejecuta el script:
```bash
python scripts/automatizacion_ingesta.py

```


4. El script validará automáticamente que los productos y fechas existan antes de insertarlos, protegiendo la integridad de la base de datos.

---

## 3. Verificación de la Base de Datos

Una vez finalizado el proceso, puedes verificar que los datos se cargaron correctamente ejecutando esta consulta en SSMS:

```sql
USE Inventario_DWH;
SELECT 
    (SELECT COUNT(*) FROM Catalogo.Dim_Producto) AS Total_Productos,
    (SELECT COUNT(*) FROM Operaciones.Fact_Ventas) AS Total_Ventas,
    (SELECT COUNT(*) FROM Operaciones.Fact_Inventario_Final) AS Total_Inventario_Final;

```

---

## Notas sobre Seguridad e Integridad

* **Filtros de Integridad**: Si el script indica que se insertaron 0 filas en alguna tabla (como el Inventario Inicial), es probable que los datos del CSV no coincidan con el catálogo maestro. Esto es un comportamiento esperado para evitar datos huérfanos.
* **Tipos de Datos**: Se utiliza el tipo `DECIMAL(18,2)` para medidas y precios, asegurando que no se pierda información decimal durante la ingesta.

---

