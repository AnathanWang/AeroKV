# AeroKV

Легковесное in-memory key-value хранилище с персистентностью.

## Возможности
- ✅ TTL (автоматическое удаление по времени)
- ✅ Персистентность (автосохранение на диск)
- ✅ Два интерфейса: TCP + Telegram Bot
- ✅ Rate limiting (защита от спама)

## Быстрый старт
1. Установите переменную окружения: `export TELEGRAM_BOT_TOKEN=your_token`
2. Запустите: `dotnet run`
3. Отправьте боту `/set mykey 60 myvalue`

## API
- `SET key ttl_seconds value` — сохранить значение
- `GET key` — получить значение
- `PING` — проверка связи
