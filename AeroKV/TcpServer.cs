using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using Serilog;

namespace AeroKV;

public class TcpServer
{
    private readonly int _port;
    private TcpListener? _listener;
    private bool _isRunning;

    public TcpServer(int port)
    {
        _port = port;
    }

    public void Start()
    {
        _listener = new TcpListener(IPAddress.Any, _port);
        _listener.Start();
        _isRunning = true;
        
        Log.Information("TCP-транспорт запущен напорту: {Port}", _port);
        Task.Run(ListenForClientsAsync);
    }

    private async Task ListenForClientsAsync()
    {
        while (_isRunning && _listener != null)
        {
            try
            {
                TcpClient client = await _listener.AcceptTcpClientAsync();
                Log.Information("Новое сетевое подключение: {RemoteEndPoint}", client.Client.RemoteEndPoint);
                _ = Task.Run(() => HandleClientAsync(client));
            }
            catch (Exception ex)
            {
                if (_isRunning) Log.Error(ex, "Ошибка при приеме TCP-клиента");
            }
        }
    }

    private async Task HandleClientAsync(TcpClient client)
    {
        using (client)
        await using (NetworkStream stream = client.GetStream())
        using (var reader = new StreamReader(stream, Encoding.UTF8))
        await using (var writer = new StreamWriter(stream, Encoding.UTF8) { AutoFlush = true })
        {
            await writer.WriteLineAsync("+CONNECTED Welcome to AeroKV TCP Interface\n");

            string? inputLine;
            while ((inputLine = await reader.ReadLineAsync()) != null)
            {
                if (string.IsNullOrWhiteSpace(inputLine)) continue;

                string response = ProcessCommand(inputLine);
                await writer.WriteLineAsync(response);
            }
        }
        Log.Information("TCP-клиент отключился");
    }

    private string ProcessCommand(string rawCommand)
    {
        var args = rawCommand.Split(' ', 4);
        string cmd = args[0].ToUpper();
        var engine = AeroKvEngine.Instance;

        try
        {
            if (cmd == "SET")
            {
                if (args.Length < 4) return "-ERR Нарушен формат. Пример: SET key 60 value";
                string key = args[1];
                if (!int.TryParse(args[2], out int ttlSeconds)) return "-ERR TTL должен быть числом";
                string value = args[3];

                engine.Set(key, value, TimeSpan.FromSeconds(ttlSeconds));
                return "+OK";
            }

            if (cmd == "GET")
            {
                if (args.Length < 2) return "-ERR Укажите ключ. Пример: GET key";
                string key = args[1];
                string? result = engine.Get(key);

                if (result != null) return $"S{result.Length}\n{result}";
                return "$-1";
            }

            if (cmd == "PING") return "+PONG";

            return "-ERR Неизвестная ошибка СУБД";
        }
        catch (Exception ex)
        {
            return $"-ERR Внутренний сбой ядра: {ex.Message}";
        }
    }

    public void Stop()
    {
        _isRunning = false;
        _listener?.Stop();
        Log.Information("TCP-транспорт остановлен");
    }
}