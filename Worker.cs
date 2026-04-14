using Microsoft.Data.SqlClient;

namespace EtlProyecto.Worker;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IConfiguration _configuration;

    public Worker(ILogger<Worker> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var connectionString = _configuration.GetConnectionString("DefaultConnection");

        try
        {
            using SqlConnection connection = new SqlConnection(connectionString);
            await connection.OpenAsync(stoppingToken);

            _logger.LogInformation("Conexión a SQL Server exitosa.");

            string insertDimCliente = @"
INSERT INTO dbo.DIM_CLIENTE
(
    cliente_id_natural,
    nombre,
    apellido,
    correo,
    telefono,
    ciudad,
    pais
)
SELECT
    c.CustomerID,
    c.FirstName,
    c.LastName,
    c.Email,
    c.Phone,
    c.City,
    c.Country
FROM dbo.STG_CUSTOMERS c
WHERE NOT EXISTS
(
    SELECT 1
    FROM dbo.DIM_CLIENTE d
    WHERE d.cliente_id_natural = c.CustomerID
);";

            using (SqlCommand commandCliente = new SqlCommand(insertDimCliente, connection))
            {
                int rowsCliente = await commandCliente.ExecuteNonQueryAsync(stoppingToken);
                _logger.LogInformation("Clientes insertados en DIM_CLIENTE: {Total}", rowsCliente);
            }

            string insertDimProducto = @"
INSERT INTO dbo.DIM_PRODUCTO
(
    producto_id_natural,
    nombre_producto,
    categoria,
    precio_lista
)
SELECT
    p.ProductID,
    p.ProductName,
    p.Category,
    p.Price
FROM dbo.STG_PRODUCTS p
WHERE NOT EXISTS
(
    SELECT 1
    FROM dbo.DIM_PRODUCTO d
    WHERE d.producto_id_natural = p.ProductID
);";

            using (SqlCommand commandProducto = new SqlCommand(insertDimProducto, connection))
            {
                int rowsProducto = await commandProducto.ExecuteNonQueryAsync(stoppingToken);
                _logger.LogInformation("Productos insertados en DIM_PRODUCTO: {Total}", rowsProducto);
            }

            string insertDimTiempo = @"
INSERT INTO dbo.DIM_TIEMPO
(
    fecha,
    anio,
    trimestre,
    mes,
    nombre_mes,
    dia,
    dia_semana,
    nombre_dia
)
SELECT
    o.OrderDate AS fecha,
    YEAR(o.OrderDate) AS anio,
    DATEPART(QUARTER, o.OrderDate) AS trimestre,
    MONTH(o.OrderDate) AS mes,
    DATENAME(MONTH, o.OrderDate) AS nombre_mes,
    DAY(o.OrderDate) AS dia,
    DATEPART(WEEKDAY, o.OrderDate) AS dia_semana,
    DATENAME(WEEKDAY, o.OrderDate) AS nombre_dia
FROM dbo.STG_ORDERS o
WHERE NOT EXISTS
(
    SELECT 1
    FROM dbo.DIM_TIEMPO d
    WHERE d.fecha = o.OrderDate
);";

            using (SqlCommand commandTiempo = new SqlCommand(insertDimTiempo, connection))
            {
                int rowsTiempo = await commandTiempo.ExecuteNonQueryAsync(stoppingToken);
                _logger.LogInformation("Fechas insertadas en DIM_TIEMPO: {Total}", rowsTiempo);
            }

            string insertDimEstado = @"
INSERT INTO dbo.DIM_ESTADO_ORDEN
(
    estado_id_natural,
    nombre_estado
)
SELECT
    ROW_NUMBER() OVER (ORDER BY x.Status) + ISNULL((SELECT MAX(estado_id_natural) FROM dbo.DIM_ESTADO_ORDEN), 0) AS estado_id_natural,
    x.Status
FROM
(
    SELECT DISTINCT Status
    FROM dbo.STG_ORDERS
) x
WHERE NOT EXISTS
(
    SELECT 1
    FROM dbo.DIM_ESTADO_ORDEN d
    WHERE d.nombre_estado = x.Status
);";

            using (SqlCommand commandEstado = new SqlCommand(insertDimEstado, connection))
            {
                int rowsEstado = await commandEstado.ExecuteNonQueryAsync(stoppingToken);
                _logger.LogInformation("Estados insertados en DIM_ESTADO_ORDEN: {Total}", rowsEstado);
            }

            string totalFactQuery = @"SELECT COUNT(*) FROM dbo.FACT_VENTAS;";

            using (SqlCommand commandFact = new SqlCommand(totalFactQuery, connection))
            {
                var totalFact = await commandFact.ExecuteScalarAsync(stoppingToken);
                _logger.LogInformation("Total actual de registros en FACT_VENTAS: {Total}", totalFact);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error al cargar dimensiones.");
        }

        await Task.Delay(1000, stoppingToken);
    }
}