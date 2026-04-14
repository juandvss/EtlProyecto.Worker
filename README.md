# ETL Proyecto

Este proyecto trabaja la carga de dimensiones de un almacén de datos en SQL Server, utilizando un Worker Service desarrollado en .NET.

El proceso toma la información desde las tablas de staging de la base de datos `AnalisisVentasETL` y carga las dimensiones principales del modelo estrella:

- `DIM_CLIENTE`
- `DIM_PRODUCTO`
- `DIM_TIEMPO`
- `DIM_ESTADO_ORDEN`

La carga se realiza de forma incremental, usando validaciones para evitar registros duplicados. Además, el programa consulta la tabla `FACT_VENTAS` para mostrar en consola la cantidad total de registros existentes y confirmar que la estructura analítica sigue disponible.

## Cómo funciona

1. El Worker se conecta a SQL Server.
2. Lee los datos desde las tablas de staging.
3. Inserta la información en las dimensiones del Data Warehouse.
4. Evita duplicados con la condición `WHERE NOT EXISTS`.
5. Muestra en consola el resultado de la ejecución y el total actual de registros en `FACT_VENTAS`.

## Tablas utilizadas

### Staging
- `STG_CUSTOMERS`
- `STG_PRODUCTS`
- `STG_ORDERS`
- `STG_ORDER_DETAILS`

### Dimensiones
- `DIM_CLIENTE`
- `DIM_PRODUCTO`
- `DIM_TIEMPO`
- `DIM_ESTADO_ORDEN`

### Tabla de hechos
- `FACT_VENTAS`

## Tecnologías utilizadas

- .NET
- C#
- SQL Server
- Visual Studio