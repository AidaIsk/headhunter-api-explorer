# 🚀 Airflow + Postgres + Adminer (Docker Compose)

## 📌 Описание
Этот проект поднимает локальное окружение для работы с **Apache Airflow** и **Postgres**.  
Airflow может использоваться для загрузки данных по API и их сохранения в Postgres.  
Adminer добавлен для удобного доступа к базе данных через веб-интерфейс.

---

## 🛠 Состав
- **Airflow Webserver** → [http://localhost:8090](http://localhost:8090)  
- **Airflow Scheduler** (выполняет DAG-и)  
- **Postgres** → база данных для Airflow и ваших DAG-ов  
- **Adminer** → [http://localhost:8091](http://localhost:8091) (UI для Postgres)
- **MINIO** →  объектное хранилище - запись данных в S3 [http://localhost:9001]

---

## ⚙️ Установка и запуск

1. Склонируй репозиторий:
   ```bash
   git clone https://github.com/<your-username>/<your-repo>.git
   cd <your-repo>
2. Создай файл окружения:
   ```bash
   cp .env.example .env
   Отредактируй .env, указав свои порты и пароли.
3. Подними контейнеры:
   ```bash
   docker compose up --build


🔑 Доступы
Airflow UI

URL: http://localhost:8090
Логин: admin
Пароль: admin (или другой из .env)

Adminer (Postgres UI)

URL: http://localhost:8091
System: PostgreSQL
Server: postgres
User: user
Password: pass
Database: mydb

📂 Структура проекта
.
├── dags/                 # твои DAG-и
├── Dockerfile            # кастомный Airflow-образ с зависимостями
├── docker-compose.yaml   # инфраструктура
├── .env.example          # пример конфигурации
├── .gitignore            # игнор лишнего

📝 Примечания

Все чувствительные данные храним только в .env.
Логи Airflow (logs/) и данные Postgres (pg_data/) не попадают в репозиторий.
DAG-и автоматически подхватываются из папки dags/.

