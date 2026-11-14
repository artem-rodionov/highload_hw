# Resilience Strategies

Учебная реализация стратегий устойчивости для HW2: одиночные клиентские стратегии и мультиклиентские стратегии над пулом реплик.

## Стратегии

| Стратегия | Интерфейс | Поведение |
|---|---|---|
| Default | `Strategy` | один подзапрос с таймаутом |
| Retries | `Strategy` | повторные попытки с учётом лимитов |
| Exponential backoff | `Strategy` | retries с растущей задержкой |
| Round-robin | `MultiStrategy` | ротация реплик, продолжение кольца между вызовами |
| Hedging | `MultiStrategy` | запуск остальных реплик после `hedgeDelay` |
| Circuit breaking | `MultiStrategy` | окно ошибок, размыкание и half-open проба |

## Структура

```
internal/adapter   SubrequestSender, Response, MockSender
internal/entity    Budget и sentinel-ошибки
internal/strategy  реализации стратегий и тесты
```

## Бюджеты

- `MaxRetries <= 0` — одна попытка.
- `0` в `FastErrors`, `Failures`, `Timeouts` — соответствующий лимит не задан.
- `TotalLatency == 0` — без общего дедлайна.
- `SubrequestLatency == 0` при заданном `TotalLatency` — используется остаток общего бюджета.
- `CircuitBreakerConfig`: `WindowSize > 0`, `0 < FailureThreshold <= 1`, `OpenTimeout > 0`.

## Запуск тестов

```bash
./test.sh
```

Скрипт проверяет `gofmt`, запускает `go vet` и тесты с `-race`.
