using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Telegram.Bot;
using Telegram.Bot.Polling;
using Telegram.Bot.Types;
using Telegram.Bot.Types.Enums;

using System.IO;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Collections.Generic;

namespace AeroKV
{
    //Структура данных: элемент хранилища
    public class AeroKvItem
    {
        public string Value { get; set; } = ""; //данные сохраненные пользователем
        public DateTime ExpirationTime { get; set; } // метка когда должно быть удалено
        
        //свойство для быстрой проверки истек ли срок жизни ttl
        [JsonIgnore]
        public bool IsExpired => DateTime.UtcNow > ExpirationTime;

        // Нужен для System.Text.Json при чтении снапшота с диска
        public AeroKvItem() { }

        public AeroKvItem(string value, TimeSpan ttl)
        {
            Value = value;
            ExpirationTime = DateTime.UtcNow.Add(ttl); // рассчитать точнее время смерти
        }
    }

    public class RateLimiter
    {
        private readonly ConcurrentDictionary<long, (DateTime WindowStart, int RequestCount)> _userRequests = new();
        private readonly int _maxRequests;
        private readonly TimeSpan _windowSize;

        public RateLimiter(int maxRequests, TimeSpan windowSize)
        {
            _maxRequests = maxRequests;
            _windowSize = windowSize;
        }

        public bool IsRequestAllowed(long userId)
        {
            DateTime now = DateTime.UtcNow;

            var userStats = _userRequests.AddOrUpdate(userId, _ => (now, 1), (_, current) =>
            {
                if (now - current.WindowStart > _windowSize)
                {
                    return (now, 1);
                }

                return (current.WindowStart, current.RequestCount + 1);
            });

            return userStats.RequestCount <= _maxRequests;
        }
    }
    
    //Движок хранения in memory key-value store (aero kv engine)
    public class AeroKvEngine
    {
        //Потокобезопасная ленивая инциализация паттерна синглтон
        private static readonly Lazy<AeroKvEngine> _instance = new(() => new AeroKvEngine());
        public static AeroKvEngine Instance => _instance.Value;
        
        //Высокопроизводительнео потокобезосное ядро хранилища
        private readonly ConcurrentDictionary<string, AeroKvItem> _store = new();

        private AeroKvEngine()
        {
            LoadFromDisk();
            Task.Run(ActiveEvictionLoopAsync);
        }

        public void Set(string key, string value, TimeSpan ttl)
        {
            _store[key] = new AeroKvItem(value, ttl);
            SaveToDisk();
        }

        public string Get(string key)
        {
            if (_store.TryGetValue(key, out var item))
            {
                if (!item.IsExpired) return item.Value;

                _store.TryRemove(key, out _);
            }

            return null;
        }

        private async Task ActiveEvictionLoopAsync()
        {
            while (true)
            {
                await Task.Delay(10000);

                foreach (var key in _store.Keys)
                {
                    if (_store.TryGetValue(key, out var item) && item.IsExpired)
                    {
                        _store.TryRemove(key, out _);
                        Console.WriteLine($"[AeroKV-Eviction] Ключ '{key}' автоматически удален из оперативной памяти (TTL Expired).");
                    }
                }
            }
        }
        
        // Файл будет гарантированно лежать в твоей домашней папке пользователя на Mac (на одном уровне с Загрузками/Документами)
        // Поднимаемся на несколько уровней вверх из папки bin прямо в корень твоего проекта
        private static readonly string DumpPath = Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "aerokv_snapshot.json");

        private readonly object _filelock = new();
        private bool _isSaving = false;
        public void SaveToDisk()
        {
            if (_store.IsEmpty) return;

            lock (_filelock)
            {
                if (_isSaving)
                {
                    Console.WriteLine("[AeroKV-Persistence] Фоновое сохранение уже выполняется. Пропускаем дублирующий запрос.");
                    return;
                }
                _isSaving = true;
            }

            var snapshot = new Dictionary<string, AeroKvItem>(_store);

            Task.Run(() =>
            {
                try
                {
                    var options = new JsonSerializerOptions { WriteIndented = true };

                    string jsonString = JsonSerializer.Serialize(_store, options);

                    lock (_filelock)
                    {
                        File.WriteAllText(DumpPath, jsonString);
                    }

                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine(
                        $"[AeroKV-BGSAVE] Фоновый снапшот успешно записан на диск ({snapshot.Count} ключей).");
                    Console.ResetColor();
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[AeroKV-Persistence Error] Ошибка фонового сохранения: {ex.Message}");
                }
                finally
                {
                    lock (_filelock)
                    {
                        _isSaving = false;
                    }
                }
            });
        }

        public void LoadFromDisk()
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine($"[AeroKV-Debug] Ищу снапшот по пути: {DumpPath}");
            Console.ResetColor();

            if (!File.Exists(DumpPath)) 
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("[AeroKV-Debug] Файл снапшота НЕ НАЙДЕН на диске. Начинаем с пустой базой.");
                Console.ResetColor();
                return;
            }

            try
            {
                string jsonString = File.ReadAllText(DumpPath);
        
                // Изменение: читаем сначала как обычный плоский Dictionary
                var deserialized = JsonSerializer.Deserialize<Dictionary<string, AeroKvItem>>(jsonString);

                if (deserialized != null)
                {
                    _store.Clear();
                    foreach (var pair in deserialized)
                    {
                        string key = pair.Key;
                        string value = pair.Value.Value;
                        DateTime expTime = pair.Value.ExpirationTime;

                        // Жестко принуждаем время быть UTC прямо в сырых данных
                        if (expTime.Kind != DateTimeKind.Utc)
                        {
                            expTime = DateTime.SpecifyKind(expTime, DateTimeKind.Utc);
                        }

                        // Считаем, сколько секунд жизни ОСТАЛОСЬ у ключа
                        TimeSpan remainingTtl = expTime - DateTime.UtcNow;

                        // Если ключ еще должен жить — создаем его в хранилище заново с чистым временем
                        if (remainingTtl.TotalSeconds > 0)
                        {
                            _store[key] = new AeroKvItem(value, remainingTtl);
                        }
                        else
                        {
                            Console.WriteLine($"[AeroKV-Persistence] Ключ '{key}' пропущен: его TTL истек, пока сервер был отключен.");
                        }
                    }

                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine($"[AeroKV-Persistence] Данные успешно восстановлены с диска. Активных ключей: {_store.Count}");
                    Console.ResetColor();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[AeroKV-Persistence Error] Не удалось прочитать снапшот: {ex.Message}");
            }
        }
    }

    class Program
    {
        private static readonly RateLimiter _limiter =
            new RateLimiter(maxRequests: 3, windowSize: TimeSpan.FromSeconds(10));
        static async Task Main(string[] args)
        {
            // Передаем токен напрямую, без лишних переменных и проверок
            var botClient = new TelegramBotClient("8934538995:AAFTIR7fI9BsD5jKGLa19xkuJd1TVmzpICM");
            using var cts = new CancellationTokenSource();

            var receiverOptions = new ReceiverOptions { AllowedUpdates = Array.Empty<UpdateType>() };
            
            botClient.StartReceiving(
                updateHandler: HandleUpdateAsync,
                errorHandler: HandlePollingErrorAsync,
                receiverOptions: receiverOptions,
                cancellationToken: cts.Token
            );

            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine("=================================================================");
            Console.WriteLine("  AeroKV Engine v1.0.0 — High-Performance In-Memory Store       ");
            Console.WriteLine("=================================================================");
            Console.ResetColor();
            Console.WriteLine("Статус сервера: ОНЛАЙН");
            Console.WriteLine("Транспортный слой: Телеграм бот апи соединен");
            Console.WriteLine("Нажмите энтер в консоли для корректной остановки ядра aeroKV \n");
            
            Console.ReadLine();
            cts.Cancel();
            // Перед полным закрытием сохраняем всё, что выжило в оперативной памяти
            Console.WriteLine("Завершение работы AeroKV... Сохраняем состояние...");
            AeroKvEngine.Instance.SaveToDisk();
        }

        private static async Task HandleUpdateAsync(ITelegramBotClient botClient, Update update,
            CancellationToken cancellationToken)
        {
            if (update.Message is not { Text: { } messageText } message) return;
            var chatId = message.Chat.Id;
            if (!_limiter.IsRequestAllowed(chatId))
            {
                await botClient.SendMessage(
                    chatId: chatId,
                    text: "🛑 *AeroKV Rate Limit Exceeded:* Ты отправляешь запросы слишком быстро! Разрешено не более 3 запросов в 10 секунд. Остынь.",
                    parseMode: ParseMode.Markdown,
                    cancellationToken: cancellationToken);

                Console.WriteLine($"[AeroKV-Security] Запрос от пользователя {chatId} заблокирован (Rate Limit).");
                return; // Мгновенно прерываем выполнение метода!
            }

            var kvEngine = AeroKvEngine.Instance;

            var args = messageText.Split(' ', 4);
            string command = args[0].ToLower();

            if (command == "/set")
            {
                if (args.Length < 4)
                {
                    await botClient.SendMessage(
                        chatId: chatId,
                        text: "Неверный формат команды! Используйте метод: /set [ключ] [время в секундах] [значение]",
                        parseMode: ParseMode.Markdown, 
                        cancellationToken: cancellationToken);
                    return;
                }

                string key = args[1];
                string ttlStr = args[2];
                string value = args[3];

                if (int.TryParse(ttlStr, out int seconds) && seconds > 0)
                {
                    kvEngine.Set(key, value, TimeSpan.FromSeconds(seconds));

                    await botClient.SendMessage(
                        chatId: chatId,
                        text: $"Данные успешно записаны в RAM!\nКлюч: `{key}`\nTTL: `{seconds}` сек.", 
                        parseMode: ParseMode.Markdown,
                        cancellationToken: cancellationToken);
                    Console.WriteLine($"Запись ключа '{key}' выполнена успешно");
                }
                else
                {
                    await botClient.SendMessage(
                        chatId: chatId, 
                        text: "Параметр времени жизни должен быть больше 0 секунд",
                        parseMode: ParseMode.Markdown, 
                        cancellationToken: cancellationToken);
                }
            }
            else if (command == "/get")
            {
                if (args.Length < 2)
                {
                    await botClient.SendMessage(
                        chatId: chatId, 
                        text: "Не указан ключ! Используйте метод: /get [ключ]",
                        parseMode: ParseMode.Markdown, 
                        cancellationToken: cancellationToken);
                    return;
                }

                string key = args[1];
                string result = kvEngine.Get(key);

                if (result != null)
                {
                    await botClient.SendMessage(
                        chatId: chatId, 
                        text: $"🚀 *AeroKV Response:*\n`{result}`", 
                        parseMode: ParseMode.Markdown,
                        cancellationToken: cancellationToken);
                    Console.WriteLine($"Чтение ключа '{key}' успешно");
                }
                else
                {
                    await botClient.SendMessage(
                        chatId: chatId,
                        text: $"Ключ `{key}` отсутствует в системе или был уничтожен по истечении срока TTL.",
                        parseMode: ParseMode.Markdown, 
                        cancellationToken: cancellationToken);
                    Console.WriteLine($"Чтение ключа '{key}' провалено (Miss/Expired)");
                }
            }
            else
            {
                string welcomeMessage = "⚡️ *Добро пожаловать в интерфейс управления AeroKV* ⚡️\n\n" +
                                        "Вы подключены к легковесному Key-Value хранилищу в оперативной памяти.\n\n" +
                                        "ℹ️ *Доступные методы API:*\n" +
                                        "🔹 `/set [ключ] [время_сек] [значение]` — Сохранить данные с таймером жизни.\n" +
                                        "🔹 `/get [ключ]` — Извлечь данные из памяти сервера.\n\n" +
                                        "_Пример использования:_\n" +
                                        "`/set session_id 45 active_user_777`\n" +
                                        "`/get session_id`";

                await botClient.SendMessage(
                    chatId: chatId, 
                    text: welcomeMessage, 
                    parseMode: ParseMode.Markdown,
                    cancellationToken: cancellationToken);
            }
        }

        private static Task HandlePollingErrorAsync(ITelegramBotClient botClient, Exception exception,
            CancellationToken cancellationToken)
        {
            Console.ForegroundColor = ConsoleColor.DarkRed;
            Console.WriteLine($"Ошибка соединения: {exception.Message}");
            Console.ResetColor();
            return Task.CompletedTask;
        }
    }
}


