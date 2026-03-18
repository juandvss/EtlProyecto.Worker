using Microsoft.Data.SqlClient;
using System.Net.Http;
using System.Text.Json;

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

            string selectQuery = "SELECT id, cliente, comentario, fecha FROM dbo.RESEÑAS_FUENTE";

            using SqlCommand selectCommand = new SqlCommand(selectQuery, connection);
            using SqlDataReader reader = await selectCommand.ExecuteReaderAsync(stoppingToken);

            _logger.LogInformation("Datos extraídos desde la base de datos:");

            while (await reader.ReadAsync(stoppingToken))
            {
                int id = reader.GetInt32(0);
                string cliente = reader.GetString(1);
                string comentario = reader.GetString(2);
                DateTime fecha = reader.GetDateTime(3);

                _logger.LogInformation("ID: {Id}, Cliente: {Cliente}, Comentario: {Comentario}", id, cliente, comentario);
            }

            await reader.CloseAsync();

            _logger.LogInformation("Conexión a SQL Server exitosa.");

            string filePath = Path.Combine(AppContext.BaseDirectory, "Data", "ventas.csv");
            var lines = await File.ReadAllLinesAsync(filePath, stoppingToken);

            for (int i = 1; i < lines.Length; i++)
            {
                var columns = lines[i].Split(',');

                string insertQuery = @"
INSERT INTO dbo.STG_VENTAS (id, cliente, producto, fecha, cantidad, precio, estado)
VALUES (@id, @cliente, @producto, @fecha, @cantidad, @precio, @estado)";

                using SqlCommand commandInsert = new SqlCommand(insertQuery, connection);

                commandInsert.Parameters.AddWithValue("@id", int.Parse(columns[0]));
                commandInsert.Parameters.AddWithValue("@cliente", columns[1]);
                commandInsert.Parameters.AddWithValue("@producto", columns[2]);
                commandInsert.Parameters.AddWithValue("@fecha", DateTime.Parse(columns[3]));
                commandInsert.Parameters.AddWithValue("@cantidad", int.Parse(columns[4]));
                commandInsert.Parameters.AddWithValue("@precio", decimal.Parse(columns[5]));
                commandInsert.Parameters.AddWithValue("@estado", columns[6]);

                await commandInsert.ExecuteNonQueryAsync(stoppingToken);
            }

            _logger.LogInformation("Datos del CSV insertados en STG_VENTAS.");

            var apiUrl = _configuration["ApiSettings:Url"];

            using HttpClient client = new HttpClient();
            var response = await client.GetAsync(apiUrl, stoppingToken);

            if (response.IsSuccessStatusCode)
            {
                var json = await response.Content.ReadAsStringAsync(stoppingToken);

                _logger.LogInformation("Datos recibidos de la API:");

                var document = JsonDocument.Parse(json);

                foreach (var item in document.RootElement.EnumerateArray())
                {
                    var name = item.GetProperty("name").GetString();
                    _logger.LogInformation("Usuario: {Nombre}", name);
                }
            }
            else
            {
                _logger.LogError("Error al consumir la API.");
            }

            string query = "SELECT COUNT(*) FROM dbo.STG_VENTAS";
            using SqlCommand command = new SqlCommand(query, connection);

            var result = await command.ExecuteScalarAsync(stoppingToken);
            _logger.LogInformation("Cantidad de registros en STG_VENTAS: {Total}", result);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error al conectar con la base de datos o procesar los datos.");
        }

        await Task.Delay(1000, stoppingToken);
    }
}