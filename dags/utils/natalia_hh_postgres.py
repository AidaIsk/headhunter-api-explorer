from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

from pathlib import Path
from datetime import datetime
import json
import pandas as pd
import math
from psycopg2.extras import execute_values

from utils.aida_hh_minio import get_s3_client

# создание таблицы в постгрес для сырых данных 
def init_postgres_tables(postgres_conn_id: str, ddl_path: str):

    ddl_path = Path(ddl_path) 
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()
    cur = conn.cursor()

    with open(ddl_path) as f:
        ddl_sql = f.read()
    
    for command in ddl_sql.split(";"):
        if command.strip():
            cur.execute(command)
    
    conn.commit()
    cur.close()
    conn.close()

# Проверка новых файлов vacancies_list в s3
def check_new_files(bucket, prefix, **context):

    # Получаем последнее время проверки из Airflow Variable или используем start_date
    last_check_str = Variable.get("LAST_S3_CHECK_HH")
    last_check = datetime.strptime(last_check_str, "%Y-%m-%d").date()

    # все файлы в S3
    s3_client = get_s3_client()

    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)


    new_keys = [
        obj["Key"]
        for obj in response.get("Contents", [])
        if obj["LastModified"].date() > last_check
    ]

    # Перезаписываем last_check
    now_str = datetime.now().strftime("%Y-%m-%d")
    Variable.set("LAST_S3_CHECK_HH", now_str)

    # Сохраняем в XCom 
    context["ti"].xcom_push(key="new_keys", value=new_keys)


# Проверка новых файлов vacancies_details в s3
def check_new_files_batch(bucket, prefix, **context):
    # Получаем последнее время проверки из Airflow Variable
    last_check_str = Variable.get("LAST_S3_CHECK_HH_BATCH")
    last_check = datetime.strptime(last_check_str, "%Y-%m-%d").date()

    # все файлы в S3
    s3_client = get_s3_client()

    # Получаем все объекты с префиксом
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    objects = response.get("Contents", [])

    # Сгруппируем файлы по дате (dt=YYYY-MM-DD)
    dates_files = {}
    for obj in objects:
        key = obj["Key"]
        # ключ вида .../dt=YYYY-MM-DD/.../part-000.jsonl
        if "/dt=" not in key:
            continue
        dt_part = key.split("/dt=")[1].split("/")[0]
        dates_files.setdefault(dt_part, []).append(obj["LastModified"])

    # Проверяем, для каких дат нужно загрузить/перезагрузить
    dates_to_reload = []
    for dt, last_modified_list in dates_files.items():
        latest_modified = max(last_modified_list)
        if latest_modified.date() > last_check:
            dates_to_reload.append(dt)

    # Перезаписываем last_check
    now_str = datetime.now().strftime("%Y-%m-%d")
    Variable.set("LAST_S3_CHECK_HH_BATCH", now_str)

    # Сохраняем в XCom 
    context["ti"].xcom_push(key="dates_to_reload", value=dates_to_reload)

    print("LAST_CHECK:", last_check)
    print("FOUND DATES:", dates_files.keys())
    print("DATES TO RELOAD:", dates_to_reload)


# замена NaN из датафрейма на None для корректной загрузки в pg json-ов 
def to_json_or_none(value):
    if value is None:
        return None

    # pandas NaN / float NaN
    if isinstance(value, float) and math.isnan(value):
        return None

    # пустые списки → NULL
    if isinstance(value, list):
        if len(value) == 0:
            return None
        return json.dumps(value, ensure_ascii=False)

    # dict → JSON
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)

    # строки "null" / "NaN"
    if value in ("null", "NaN"):
        return None

    return json.dumps(value, ensure_ascii=False)

# правильная конвертация отсутствующих значений в колонках булевого типа
def to_bool_or_none(value):
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    if value in ("NaN", "null"):
        return None
    return bool(value)

# загрузка данных из минио в постгрес
def load_to_postgres(**context):

    ti = context['ti']
    new_keys = ti.xcom_pull(task_ids='check_new_files', key='new_keys')

    if not new_keys:
        print("Нет новых файлов для загрузки")
        return

    hook = PostgresHook(postgres_conn_id="postgres_bronze")
    conn = hook.get_conn()
    cur = conn.cursor()

    #hook = S3Hook(aws_conn_id="minio_hh")
    bucket = "hh-raw"
    s3_client = get_s3_client()

    VACANCIES_UPSERT_SQL = """
                INSERT INTO bronze.hh_vacancies_bronze (
                    id, premium, name, department, has_test, response_letter_required,
                    area, salary, salary_range, type, address,
                    response_url, sort_point_distance, published_at, created_at, archived,
                    apply_alternate_url, show_logo_in_search, show_contacts, insider_interview,
                    url, alternate_url, relations, employer, snippet, contacts,
                    schedule, working_days, working_time_intervals, working_time_modes, accept_temporary,
                    fly_in_fly_out_duration, work_format, working_hours, work_schedule_by_days, night_shifts,
                    professional_roles, accept_incomplete_resumes, experience, employment, employment_form,
                    internship, adv_response_url, is_adv_vacancy, adv_context,
                    branding, brand_snippet, search_profile, expected_risk_category,
                    load_dt, load_type
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE
                SET 
                    premium = EXCLUDED.premium,
                    name = EXCLUDED.name,
                    department = EXCLUDED.department,
                    has_test = EXCLUDED.has_test,
                    response_letter_required = EXCLUDED.response_letter_required,
                    area = EXCLUDED.area,
                    salary = EXCLUDED.salary,
                    salary_range = EXCLUDED.salary_range,
                    type = EXCLUDED.type,
                    address = EXCLUDED.address,
                    response_url = EXCLUDED.response_url,
                    sort_point_distance = EXCLUDED.sort_point_distance,
                    published_at = EXCLUDED.published_at,
                    created_at = EXCLUDED.created_at,
                    archived = EXCLUDED.archived,
                    apply_alternate_url = EXCLUDED.apply_alternate_url,
                    show_logo_in_search = EXCLUDED.show_logo_in_search,
                    show_contacts = EXCLUDED.show_contacts,
                    insider_interview = EXCLUDED.insider_interview,
                    url = EXCLUDED.url,
                    alternate_url = EXCLUDED.alternate_url,
                    relations = EXCLUDED.relations,
                    employer = EXCLUDED.employer,
                    snippet = EXCLUDED.snippet,
                    contacts = EXCLUDED.contacts,
                    schedule = EXCLUDED.schedule,
                    working_days = EXCLUDED.working_days,
                    working_time_intervals = EXCLUDED.working_time_intervals,
                    working_time_modes = EXCLUDED.working_time_modes,
                    accept_temporary = EXCLUDED.accept_temporary,
                    fly_in_fly_out_duration = EXCLUDED.fly_in_fly_out_duration,
                    work_format = EXCLUDED.work_format,
                    working_hours = EXCLUDED.working_hours,
                    work_schedule_by_days = EXCLUDED.work_schedule_by_days,
                    night_shifts = EXCLUDED.night_shifts,
                    professional_roles = EXCLUDED.professional_roles,
                    accept_incomplete_resumes = EXCLUDED.accept_incomplete_resumes,
                    experience = EXCLUDED.experience,
                    employment = EXCLUDED.employment,
                    employment_form = EXCLUDED.employment_form,
                    internship = EXCLUDED.internship,
                    adv_response_url = EXCLUDED.adv_response_url,
                    is_adv_vacancy = EXCLUDED.is_adv_vacancy,
                    adv_context = EXCLUDED.adv_context,
                    branding = EXCLUDED.branding,
                    brand_snippet = EXCLUDED.brand_snippet,
                    search_profile = EXCLUDED.search_profile,
                    expected_risk_category = EXCLUDED.expected_risk_category,
                    load_dt = EXCLUDED.load_dt,
                    load_type = EXCLUDED.load_type
            """

    for key in new_keys:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        lines = obj['Body'].read().decode("utf-8").splitlines()
        data = [json.loads(line) for line in lines]
        df = pd.DataFrame(data)

        for _, row in df.iterrows():
            cur.execute(VACANCIES_UPSERT_SQL, 
                (row.get('id'), row.get('premium'), row.get('name'), to_json_or_none(row.get('department')),
                row.get('has_test'), row.get('response_letter_required'),
                to_json_or_none(row.get('area')), to_json_or_none(row.get('salary')), to_json_or_none(row.get('salary_range')),
                to_json_or_none(row.get('type')), to_json_or_none(row.get('address')),
                row.get('response_url'), row.get('sort_point_distance'), row.get('published_at'), row.get('created_at'),
                row.get('archived'), row.get('apply_alternate_url'), to_bool_or_none(row.get('show_logo_in_search')),
                row.get('show_contacts'), to_json_or_none(row.get('insider_interview')),
                row.get('url'), row.get('alternate_url'), to_json_or_none(row.get('relations')), to_json_or_none(row.get('employer')),
                to_json_or_none(row.get('snippet')), to_json_or_none(row.get('contacts')),
                to_json_or_none(row.get('schedule')), to_json_or_none(row.get('working_days')), to_json_or_none(row.get('working_time_intervals')),
                to_json_or_none(row.get('working_time_modes')), row.get('accept_temporary'), to_json_or_none(row.get('fly_in_fly_out_duration')),
                to_json_or_none(row.get('work_format')), to_json_or_none(row.get('working_hours')),
                to_json_or_none(row.get('work_schedule_by_days')), row.get('night_shifts'), to_json_or_none(row.get('professional_roles')),
                row.get('accept_incomplete_resumes'), to_json_or_none(row.get('experience')), to_json_or_none(row.get('employment')),
                to_json_or_none(row.get('employment_form')), row.get('internship'), row.get('adv_response_url'),
                row.get('is_adv_vacancy'), to_json_or_none(row.get('adv_context')), to_json_or_none(row.get('branding')),
                to_json_or_none(row.get('brand_snippet')), row.get('search_profile'), row.get('expected_risk_category'),
                row.get('load_dt'), row.get('load_type')
            ))


    conn.commit()
    cur.close()
    conn.close()


def df_to_tuples(df: pd.DataFrame):
    rows = []

    for _, row in df.iterrows():
        rows.append((
            row.get("id"),
            row.get("premium"),
            row.get("name"),
            to_json_or_none(row.get("department")),
            row.get("has_test"),
            row.get("response_letter_required"),

            to_json_or_none(row.get("area")),
            to_json_or_none(row.get("salary")),
            to_json_or_none(row.get("salary_range")),
            to_json_or_none(row.get("type")),
            to_json_or_none(row.get("address")),

            row.get("response_url"),
            row.get("sort_point_distance"),
            row.get("published_at"),
            row.get("created_at"),
            row.get("archived"),

            row.get("apply_alternate_url"),
            to_bool_or_none(row.get("show_logo_in_search")),
            row.get("show_contacts"),
            to_json_or_none(row.get("insider_interview")),

            row.get("url"),
            row.get("alternate_url"),
            to_json_or_none(row.get("relations")),
            to_json_or_none(row.get("employer")),
            to_json_or_none(row.get("snippet")),
            to_json_or_none(row.get("contacts")),

            to_json_or_none(row.get("schedule")),
            to_json_or_none(row.get("working_days")),
            to_json_or_none(row.get("working_time_intervals")),
            to_json_or_none(row.get("working_time_modes")),
            row.get("accept_temporary"),

            to_json_or_none(row.get("fly_in_fly_out_duration")),
            to_json_or_none(row.get("work_format")),
            to_json_or_none(row.get("working_hours")),
            to_json_or_none(row.get("work_schedule_by_days")),
            row.get("night_shifts"),

            to_json_or_none(row.get("professional_roles")),
            row.get("accept_incomplete_resumes"),

            to_json_or_none(row.get("experience")),
            to_json_or_none(row.get("employment")),
            to_json_or_none(row.get("employment_form")),

            row.get("internship"),
            row.get("adv_response_url"),
            row.get("is_adv_vacancy"),
            to_json_or_none(row.get("adv_context")),

            to_json_or_none(row.get("branding")),
            to_json_or_none(row.get("brand_snippet")),

            row.get("search_profile"),
            row.get("expected_risk_category"),
            row.get("load_dt"),
            row.get("load_type"),
        ))

    return rows


def load_to_postgres_batch(bucket, prefix, **context):

    ti = context['ti']
    dates_to_reload = ti.xcom_pull(task_ids='check_new_files_batch', key='dates_to_reload')

    if not dates_to_reload:
        print("Нет новых файлов для загрузки")
        return
    
    s3_client = get_s3_client()

    VACANCIES_DETAILS_UPSERT_SQL = """
        INSERT INTO bronze.hh_vacancies_details_bronze (
            id, premium, name, department, has_test, response_letter_required,
            area, salary, salary_range, type, address,
            response_url, sort_point_distance, published_at, created_at, archived,
            apply_alternate_url, show_logo_in_search, show_contacts, insider_interview,
            url, alternate_url, relations, employer, snippet, contacts,
            schedule, working_days, working_time_intervals, working_time_modes, accept_temporary,
            fly_in_fly_out_duration, work_format, working_hours, work_schedule_by_days, night_shifts,
            professional_roles, accept_incomplete_resumes, experience, employment, employment_form,
            internship, adv_response_url, is_adv_vacancy, adv_context,
            branding, brand_snippet, search_profile, expected_risk_category,
            load_dt, load_type
        ) VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            premium = EXCLUDED.premium,
            name = EXCLUDED.name,
            department = EXCLUDED.department,
            has_test = EXCLUDED.has_test,
            response_letter_required = EXCLUDED.response_letter_required,
            area = EXCLUDED.area,
            salary = EXCLUDED.salary,
            salary_range = EXCLUDED.salary_range,
            type = EXCLUDED.type,
            address = EXCLUDED.address,
            response_url = EXCLUDED.response_url,
            sort_point_distance = EXCLUDED.sort_point_distance,
            published_at = EXCLUDED.published_at,
            created_at = EXCLUDED.created_at,
            archived = EXCLUDED.archived,
            apply_alternate_url = EXCLUDED.apply_alternate_url,
            show_logo_in_search = EXCLUDED.show_logo_in_search,
            show_contacts = EXCLUDED.show_contacts,
            insider_interview = EXCLUDED.insider_interview,
            url = EXCLUDED.url,
            alternate_url = EXCLUDED.alternate_url,
            relations = EXCLUDED.relations,
            employer = EXCLUDED.employer,
            snippet = EXCLUDED.snippet,
            contacts = EXCLUDED.contacts,
            schedule = EXCLUDED.schedule,
            working_days = EXCLUDED.working_days,
            working_time_intervals = EXCLUDED.working_time_intervals,
            working_time_modes = EXCLUDED.working_time_modes,
            accept_temporary = EXCLUDED.accept_temporary,
            fly_in_fly_out_duration = EXCLUDED.fly_in_fly_out_duration,
            work_format = EXCLUDED.work_format,
            working_hours = EXCLUDED.working_hours,
            work_schedule_by_days = EXCLUDED.work_schedule_by_days,
            night_shifts = EXCLUDED.night_shifts,
            professional_roles = EXCLUDED.professional_roles,
            accept_incomplete_resumes = EXCLUDED.accept_incomplete_resumes,
            experience = EXCLUDED.experience,
            employment = EXCLUDED.employment,
            employment_form = EXCLUDED.employment_form,
            internship = EXCLUDED.internship,
            adv_response_url = EXCLUDED.adv_response_url,
            is_adv_vacancy = EXCLUDED.is_adv_vacancy,
            adv_context = EXCLUDED.adv_context,
            branding = EXCLUDED.branding,
            brand_snippet = EXCLUDED.brand_snippet,
            search_profile = EXCLUDED.search_profile,
            expected_risk_category = EXCLUDED.expected_risk_category,
            load_dt = EXCLUDED.load_dt,
            load_type = EXCLUDED.load_type
    """

    hook = PostgresHook(postgres_conn_id="postgres_bronze")
    conn = hook.get_conn()
    cur = conn.cursor()

    for dt in dates_to_reload:
        print(f"Загружаем дату {dt}")

        df_prefix = f"{prefix}/dt={dt}/"

        response = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=df_prefix
        )

        objects = response.get("Contents", [])
        if not objects:
            print(f"Нет файлов за {dt}")
            continue

        dfs = []

        for obj in objects:
            key = obj["Key"]

            if not key.endswith(".jsonl"):
                continue

            body = s3_client.get_object(
                Bucket=bucket,
                Key=key
            )["Body"]

            lines = body.read().decode("utf-8").splitlines()
            data = []
            for i, line in enumerate(lines, start=1):
                try:
                    data.append(json.loads(line))
                except json.JSONDecodeError as e:
                    print(f"[WARNING] Битая строка JSON в файле {key}, строка {i}: {e}")
                    print(f"Содержимое (обрезано до 200 символов): {line[:200]}")

            if data:
                dfs.append(pd.DataFrame(data))

        if not dfs:
            print(f"Нет данных в батчах за {dt}")
            continue

        df = pd.concat(dfs, ignore_index=True)

        rows = df_to_tuples(df)

        execute_values(
            cur,
            VACANCIES_DETAILS_UPSERT_SQL,
            rows,
            page_size=1000
        )

        conn.commit()
        print(f"Дата {dt} успешно загружена")

    cur.close()
    conn.close()


    