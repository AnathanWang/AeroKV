using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Telegram.Bot;
using Telegram.Bot.Polling;
using Telegram.Bot.Types;
using Telegram.Bot.Types.Enums;

namespace AeroKV
{
    //Структура данных: элемент хранилища
    public class AeroKvItem
    {
        public string Value { get; set; } //данные сохраненные пользователем
        public DateTime ExpirationTime { get; set; } // метка когда должно быть удалено
        
        //свойство для быстрой проверки истек ли срок жизни ttl
        public bool IsExpired => DateTime.UtcNow > ExpirationTime;

        public AeroKvItem(string value, TimeSpan ttl)
        {
            Value = value;
            ExpirationTime = DateTime.UtcNow.Add(ttl); // рассчитать точнее время смерти
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
            Task.Run(ActiveEvictionLoopAsync);
        }

        public void Set(string key, string value, TimeSpan ttl)
        {
            _store[key] = new AeroKvItem(value, ttl);
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
    }

    class Program
    {
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
        }

        private static async Task HandleUpdateAsync(ITelegramBotClient botClient, Update update,
            CancellationToken cancellationToken)
        {
            if (update.Message is not { Text: { } messageText } message) return;
            var chatId = message.Chat.Id;

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


