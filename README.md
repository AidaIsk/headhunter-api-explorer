# headhunter-api-explorer

## Обзор проекта

End-to-end Data Engineering пайплайн для сбора и подготовки данных о рынке вакансий  
с акцентом на архитектуру, качество данных и compliance-ориентированные сценарии использования.

**Поток данных:**

HeadHunter API → Airflow → MinIO (Bronze) → Postgres (Landing / Staging) → dbt (Staging → Silver → Gold) → BI

Проект демонстрирует практический, приближённый к продакшену подход к построению data-платформ:
- слоистая архитектура  
- идемпотентные загрузки  
- наблюдаемость  
- обязательные проверки качества данных  

**Технологический стек:**
- Python  
- Apache Airflow  
- MinIO (S3-совместимое хранилище)  
- Postgres  
- Docker  
- dbt (используется для staging и аналитических слоёв)

---

## Описание проекта

Проект реализует Data Engineering платформу для автоматизированного сбора, хранения  
и подготовки данных о вакансиях из HeadHunter API.

Архитектура построена по принципам, применяемым в реальных data-платформах:
- чёткое разделение ingestion и трансформаций  
- хранение raw-данных как источника истины  
- воспроизводимые и безопасные повторные запуски пайплайнов  

Помимо классической аналитики рынка труда, проект ориентирован на **Compliance и RegTech-кейсы**:
- анализ работодателей  
- анализ отраслей и географии  
- выявление потенциально подозрительных формулировок в описаниях вакансий  

---

## Архитектура пайплайна

```text
HeadHunter API
      ↓
Airflow (DAG-и ingestion)
      ↓
MinIO (Bronze слой, JSONL, партиционирование по дате)
      ↓
Airflow (загрузка и проверки)
      ↓
Postgres (Landing / Staging)
      ↓
dbt (Staging → Silver → Gold)
      ↓
BI / дашборды
```

---

## Архитектурные принципы

- Bronze слой — **source of truth**
- Raw-данные не модифицируются после загрузки
- Ingestion строго отделён от трансформаций
- Daily-загрузки отделены от init / backfill
- Все пайплайны идемпотентны
- Контроль покрытия и качества данных обязателен
- Обогащённые записи self-contained и трассируемы

---

## Architecture & Ownership

Проект спроектирован как production-like платформа данных
с чётким разделением ответственности между ingestion,
технической нормализацией и аналитическим моделированием.

Архитектура построена по принципу Medallion:
Bronze → Staging → Silver → Gold.

### Архитектура верхнего уровня

• Source of Truth:
  - MinIO используется для хранения неизменяемого Bronze-слоя (raw JSONL)
  - PostgreSQL применяется как Landing-слой и аналитическое хранилище

• Оркестрация:
  - Airflow отвечает исключительно за оркестрацию пайплайнов
  - Логика ingestion строго отделена от трансформаций

• Трансформации:
  - dbt используется только для детерминированных трансформаций данных
  - dbt не участвует в процессе ingestion

---

### Зоны ответственности

**Aida Iskakova — Architecture / Data Engineering / Compliance**

• Архитектура ingestion и дизайн пайплайнов  
• Manifest-based ingestion и контроль полноты данных (Expected vs Loaded)  
• Логика observability: coverage, severity (OK / WARNING / CRITICAL)  
• Формирование и поддержка контрактов входных данных для dbt (sources)  
• Полная ответственность за staging-слой (техническая нормализация, без бизнес-логики)  
• Обеспечение принципов immutability, idempotency и воспроизводимости пайплайнов  

**Natalia Tarasova — Analytics Engineering / Monitoring**

• Проектирование аналитической модели данных (Silver-слой)  
• Реализация бизнес-сущностей и связей между ними  
• Разработка compliance- и risk-oriented логики (risk flags)  
• Построение Gold-слоя и аналитических витрин  
• Настройка алертинга и мониторинга качества данных  
• Контроль стабильности и корректности downstream-слоёв  

---

## Orchestration & Reliability (Airflow)

Оркестрация пайплайнов построена на принципе явных зависимостей между DAG
и контролируемых failure-сценариев, без неявных предположений
о наличии данных.

### Dependency-driven orchestration

Основной ingestion DAG (`aida_hh_daily_json`) отвечает за:
- сбор raw-данных из HeadHunter API
- формирование manifest ID (контракт ожидаемых данных)

После успешного формирования manifest основной DAG
явно триггерит enrichment DAG (`aida_hh_details_daily`)
с использованием `TriggerDagRunOperator`.

Такой подход обеспечивает:
- запуск enrichment строго после готовности входных данных
- отсутствие зависимости от расписания downstream-DAG’ов
- исключение race conditions между слоями пайплайна

### Smart SKIP и failure semantics

Enrichment DAG начинается с guard-задачи, проверяющей наличие
manifest-файла в MinIO за конкретную дату загрузки.

Если manifest отсутствует:
- enrichment-задачи не выполняются
- DAG корректно завершается со статусом `SKIPPED`
  (через `AirflowSkipException`)
- ложные ошибки и алерты исключаются

Такое поведение позволяет:
- отличать отсутствие данных от реальных ошибок
- безопасно выполнять backfill и recovery-запуски
- поддерживать чистую и интерпретируемую observability

Оркестрация спроектирована таким образом, чтобы система
оставалась устойчивой и предсказуемой даже при неполных
или отсутствующих данных.

___

## Загрузка данных из HeadHunter API
### Список вакансий
На первом этапе формируются списки вакансий на основе search-профилей.
Эти датасеты используются как **manifest** для последующего обогащения и задают
ожидаемый объём данных для каждой загрузки.

### Детали вакансий (реализовано)
Для каждого vacancy_id выполняется запрос к эндпоинту:

```text
/vacancies/{id}
```
После чего данные сохраняются отдельным Bronze-датасетом.

### Загружаемые атрибуты включают:

- полное описание вакансии
- ключевые навыки
- профессиональные роли и специализации
- адрес, метро и географические координаты (при наличии)
- расширенные данные работодателя
- поведенческие маркеры (premium, тесты, требования к отклику)
- временные метки жизненного цикла вакансии (created, published, archived)

Детали вакансий хранятся отдельно от списков, формируя обогащённый Bronze-слой,
пригодный для текстового анализа, compliance-проверок и downstream-трансформаций.

---

## Качество данных и наблюдаемость
### Реализовано
- Проверка **expected vs loaded** (покрытие данных)
- Логирование на уровне батчей
- Классификация HTTP-ошибок:
  - not_found (404)
  - rate_limited (429)
  - http_error
  - request_exception
  - json_decode_error
- Отдельный Bronze-датасет с ошибочными записями
- Определение severity каждого прогона: OK, WARNING, CRITICAL
- Идемпотентная логика загрузки с безопасным повторным запуском

### Пример логов:

```text
[batch 007] ok=199 failed=1 status_counts={404: 1}
[TOTAL] expected=2803 ok=2793 failed=10 severity=WARNING
```
##  Airflow DAG-и
| DAG | Тип запуска | Назначение |
|-----|------------|------------|
| `aida_hh_init_bronze_json` | manual | Первичная загрузка и backfill вакансий |
| `aida_hh_daily_json` | scheduled (daily) | Инкрементальная загрузка списков вакансий |
| `aida_hh_details_daily` | scheduled (daily) | Загрузка и обогащение деталей вакансий |
| `nataliia_hh_to_postgres` | scheduled (daily) | Перенос Bronze-данных в Postgres |
| `sanctions_load` | planned | Загрузка санкционных списков |
| `dbt_transform_run` | planned | dbt-модели Silver / Gold и тесты |

---

##  Структура репозитория
```text
## Структура репозитория

.
├── configs/
│   └── search_profiles.yaml        # Конфигурация поисковых профилей (data-driven ingestion)
│
├── dags/
│   ├── aida_hh_init.py             # Инициализация (init / backfill сценарии)
│   ├── aida_hh_daily_dag.py        # Daily DAG: сбор списка вакансий (IDs)
│   ├── aida_hh_details_daily_dag.py# Daily DAG: enrichment вакансий (details)
│   ├── natalia_hh_postgres_dag.py  # Загрузка Bronze → Postgres (Landing / Staging)
│   ├── test_dag.py                 # Тестовый / экспериментальный DAG
│   │
│   ├── utils/
│   │   ├── aida_hh_api.py          # Работа с HH API (requests, rate limits)
│   │   ├── aida_hh_minio.py        # Клиенты и операции с MinIO
│   │   ├── hh_ids.py               # Формирование manifest (vacancy IDs)
│   │   ├── hh_details.py           # Enrichment + coverage / failure tracking
│   │   ├── search_profiles.py      # Загрузка и обработка search_profiles.yaml
│   │   └── natalia_hh_postgres.py  # Логика загрузки данных в Postgres
│   │
│   └── legacy/
│       ├── aida_hh_init_dag.py      # Устаревшие DAG'и (сохранены для истории)
│       └── minio_pandas_dag.py
│
├── dbt/
│   └── hh_compliance_dbt/
│       ├── dbt_project.yml
│       ├── packages.yml
│       ├── models/
│       │   ├── staging/
│       │   │   └── hh/
│       │   │       ├── stg_hh__vacancy_details.sql
│       │   │       ├── stg_hh__employers.sql
│       │   │       ├── stg_hh__vacancy_skills.sql
│       │   │       └── stg_hh__vacancy_text.sql
│       │   ├── silver/             # Бизнес-сущности (в разработке)
│       │   └── gold/               # Аналитические витрины (в разработке)
│       │
│       └── tests/                  # dbt tests (schema / data tests)
│
├── Dockerfile                      # Docker-образ Airflow
├── docker-compose.yml              # Локальный запуск Airflow + MinIO + Postgres
├── .env.example                    # Пример переменных окружения
├── .gitignore
└── README.md
```

dbt-часть проекта вынесена в отдельную директорию и логически отделена от ingestion-кода, что отражает разделение ответственности между сбором данных и аналитическим моделированием.

## Команда проекта

Проект реализуется в формате командной разработкии с чётко зафиксированными зонами архитектурной и аналитической ответственности.

Подробное разделение ролей, архитектурных обязанностей и контрактов между слоями описано в разделе **Architecture & Ownership**.

---

## Roadmap
- Загрузка санкционных списков (EU / UK)
- dbt-модели Silver и Gold
- Compliance-ориентированные витрины данных
- BI-дашборды (Metabase / Superset)
- Расширенные проверки качества данных через dbt

---

## Итог
Проект демонстрирует системный и приближённый к продакшену подход
к построению Data Engineering платформы с акцентом на архитектуру,
надёжность и качество данных.

Репозиторий отражает практики командной разработки, наблюдаемость пайплайнов
и готовность данных к аналитическим и compliance-сценариям использования.
