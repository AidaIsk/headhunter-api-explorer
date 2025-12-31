from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from pathlib import Path
from datetime import datetime
import json
import pandas as pd
import math

from utils.aida_hh_minio import get_s3_client


DDL_PATH = Path(__file__).parent / "ddl_pg_bronze.sql"

# создание таблицы в постгрес для сырых данных 
def init_postgres_tables():
    hook = PostgresHook(postgres_conn_id="postgres_bronze")
    conn = hook.get_conn()
    cur = conn.cursor()

    with open(DDL_PATH) as f:
        ddl_sql = f.read()

    for command in ddl_sql.split(";"):
        if command.strip():
            cur.execute(command)

    conn.commit()
    cur.close()
    conn.close()

# Проверка новых файлов в s3
def check_new_files(**context):
    #hook = S3Hook(aws_conn_id="minio_hh")

    prefix = "bronze/hh/vacancies_list/load_type=daily"
    bucket = "hh-raw"

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


# замена NaN из датафрейма на None для корректной загрузки в pg json-ов 
def to_json_or_none(value):
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if pd.isna(value):
        return None
    return to_json_or_none(value, ensure_ascii=False)

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

    for key in new_keys:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        lines = obj['Body'].read().decode("utf-8").splitlines()
        data = [json.loads(line) for line in lines]
        df = pd.DataFrame(data)

        for _, row in df.iterrows():
            cur.execute("""
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
            """, (
                row.get('id'), row.get('premium'), row.get('name'), to_json_or_none(row.get('department')),
                row.get('has_test'), row.get('response_letter_required'),
                to_json_or_none(row.get('area')), to_json_or_none(row.get('salary')), to_json_or_none(row.get('salary_range')),
                to_json_or_none(row.get('type')), to_json_or_none(row.get('address')),
                row.get('response_url'), row.get('sort_point_distance'), row.get('published_at'), row.get('created_at'),
                row.get('archived'), row.get('apply_alternate_url'), row.get('show_logo_in_search'),
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
