from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

from pathlib import Path
from datetime import datetime
import json
import pandas as pd
import math
from psycopg2.extras import execute_values
import logging
import requests

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
            row.get("id"),                                 # id
            row.get("premium"),                            # premium
            to_json_or_none(row.get("billing_type")),      # billing_type
            to_json_or_none(row.get("relations")),         # relations
            row.get("name"),                               # name
            to_json_or_none(row.get("insider_interview")), # insider_interview
            row.get("response_letter_required"),           # response_letter_required
            to_json_or_none(row.get("area")),               # area
            to_json_or_none(row.get("salary")),             # salary
            to_json_or_none(row.get("salary_range")),       # salary_range
            to_json_or_none(row.get("type")),               # type
            to_json_or_none(row.get("address")),            # address
            row.get("allow_messages"),                     # allow_messages
            to_json_or_none(row.get("experience")),        # experience
            to_json_or_none(row.get("schedule")),          # schedule
            to_json_or_none(row.get("employment")),        # employment
            to_json_or_none(row.get("department")),        # department
            row.get("show_contacts"),                      # show_contacts
            to_json_or_none(row.get("contacts")),          # contacts
            row.get("description"),                        # description
            row.get("branded_description"),                # branded_description
            to_json_or_none(row.get("vacancy_constructor_template")),  # vacancy_constructor_template
            to_json_or_none(row.get("key_skills")),         # key_skills
            to_json_or_none(row.get("auto_response")),      # auto_response
            row.get("accept_handicapped"),                 # accept_handicapped
            row.get("accept_kids"),                         # accept_kids
            row.get("age_restriction"),                     # age_restriction
            row.get("archived"),                            # archived
            row.get("response_url"),                        # response_url
            to_json_or_none(row.get("specializations")),    # specializations
            to_json_or_none(row.get("professional_roles")), # professional_roles
            row.get("code"),                                # code
            row.get("hidden"),                              # hidden
            row.get("quick_responses_allowed"),             # quick_responses_allowed
            to_json_or_none(row.get("driver_license_types")), # driver_license_types
            row.get("accept_incomplete_resumes"),           # accept_incomplete_resumes
            to_json_or_none(row.get("employer")),           # employer
            row.get("published_at"),                        # published_at
            row.get("created_at"),                          # created_at
            row.get("initial_created_at"),                  # initial_created_at
            row.get("negotiations_url"),                    # negotiations_url
            row.get("suitable_resumes_url"),                # suitable_resumes_url
            row.get("apply_alternate_url"),                 # apply_alternate_url
            row.get("has_test"),                             # has_test
            to_json_or_none(row.get("test")),               # test
            row.get("alternate_url"),                       # alternate_url
            to_json_or_none(row.get("working_days")),       # working_days
            to_json_or_none(row.get("working_time_intervals")), # working_time_intervals
            to_json_or_none(row.get("working_time_modes")), # working_time_modes
            row.get("accept_temporary"),                    # accept_temporary
            to_json_or_none(row.get("languages")),          # languages
            row.get("approved"),                            # approved
            to_json_or_none(row.get("employment_form")),    # employment_form
            to_json_or_none(row.get("fly_in_fly_out_duration")), # fly_in_fly_out_duration
            row.get("internship"),                          # internship
            row.get("night_shifts"),                        # night_shifts
            to_json_or_none(row.get("work_format")),        # work_format
            to_json_or_none(row.get("work_schedule_by_days")), # work_schedule_by_days
            to_json_or_none(row.get("working_hours")),      # working_hours
            to_bool_or_none(row.get("show_logo_in_search")),   # show_logo_in_search
            row.get("closed_for_applicants"),               # closed_for_applicants
            row.get("search_profile"),                      # search_profile
            row.get("expected_risk_category"),              # expected_risk_category
            row.get("load_dt"),                             # load_dt
            row.get("load_type"),                           # load_type
            row.get("batch_idx"),                           # batch_idx
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
        id,
        premium,
        billing_type,
        relations,
        name,
        insider_interview,
        response_letter_required,
        area,
        salary,
        salary_range,
        type,
        address,
        allow_messages,
        experience,
        schedule,
        employment,
        department,
        show_contacts,
        contacts,
        description,
        branded_description,
        vacancy_constructor_template,
        key_skills,
        auto_response,
        accept_handicapped,
        accept_kids,
        age_restriction,
        archived,
        response_url,
        specializations,
        professional_roles,
        code,
        hidden,
        quick_responses_allowed,
        driver_license_types,
        accept_incomplete_resumes,
        employer,
        published_at,
        created_at,
        initial_created_at,
        negotiations_url,
        suitable_resumes_url,
        apply_alternate_url,
        has_test,
        test,
        alternate_url,
        working_days,
        working_time_intervals,
        working_time_modes,
        accept_temporary,
        languages,
        approved,
        employment_form,
        fly_in_fly_out_duration,
        internship,
        night_shifts,
        work_format,
        work_schedule_by_days,
        working_hours,
        show_logo_in_search,
        closed_for_applicants,
        search_profile,
        expected_risk_category,
        load_dt,
        load_type,
        batch_idx
    ) VALUES %s
    ON CONFLICT (id) DO UPDATE SET
        premium = EXCLUDED.premium,
        billing_type = EXCLUDED.billing_type,
        relations = EXCLUDED.relations,
        name = EXCLUDED.name,
        insider_interview = EXCLUDED.insider_interview,
        response_letter_required = EXCLUDED.response_letter_required,
        area = EXCLUDED.area,
        salary = EXCLUDED.salary,
        salary_range = EXCLUDED.salary_range,
        type = EXCLUDED.type,
        address = EXCLUDED.address,
        allow_messages = EXCLUDED.allow_messages,
        experience = EXCLUDED.experience,
        schedule = EXCLUDED.schedule,
        employment = EXCLUDED.employment,
        department = EXCLUDED.department,
        show_contacts = EXCLUDED.show_contacts,
        contacts = EXCLUDED.contacts,
        description = EXCLUDED.description,
        branded_description = EXCLUDED.branded_description,
        vacancy_constructor_template = EXCLUDED.vacancy_constructor_template,
        key_skills = EXCLUDED.key_skills,
        auto_response = EXCLUDED.auto_response,
        accept_handicapped = EXCLUDED.accept_handicapped,
        accept_kids = EXCLUDED.accept_kids,
        age_restriction = EXCLUDED.age_restriction,
        archived = EXCLUDED.archived,
        response_url = EXCLUDED.response_url,
        specializations = EXCLUDED.specializations,
        professional_roles = EXCLUDED.professional_roles,
        code = EXCLUDED.code,
        hidden = EXCLUDED.hidden,
        quick_responses_allowed = EXCLUDED.quick_responses_allowed,
        driver_license_types = EXCLUDED.driver_license_types,
        accept_incomplete_resumes = EXCLUDED.accept_incomplete_resumes,
        employer = EXCLUDED.employer,
        published_at = EXCLUDED.published_at,
        created_at = EXCLUDED.created_at,
        initial_created_at = EXCLUDED.initial_created_at,
        negotiations_url = EXCLUDED.negotiations_url,
        suitable_resumes_url = EXCLUDED.suitable_resumes_url,
        apply_alternate_url = EXCLUDED.apply_alternate_url,
        has_test = EXCLUDED.has_test,
        test = EXCLUDED.test,
        alternate_url = EXCLUDED.alternate_url,
        working_days = EXCLUDED.working_days,
        working_time_intervals = EXCLUDED.working_time_intervals,
        working_time_modes = EXCLUDED.working_time_modes,
        accept_temporary = EXCLUDED.accept_temporary,
        languages = EXCLUDED.languages,
        approved = EXCLUDED.approved,
        employment_form = EXCLUDED.employment_form,
        fly_in_fly_out_duration = EXCLUDED.fly_in_fly_out_duration,
        internship = EXCLUDED.internship,
        night_shifts = EXCLUDED.night_shifts,
        work_format = EXCLUDED.work_format,
        work_schedule_by_days = EXCLUDED.work_schedule_by_days,
        working_hours = EXCLUDED.working_hours,
        show_logo_in_search = EXCLUDED.show_logo_in_search,
        closed_for_applicants = EXCLUDED.closed_for_applicants,
        search_profile = EXCLUDED.search_profile,
        expected_risk_category = EXCLUDED.expected_risk_category,
        load_dt = EXCLUDED.load_dt,
        load_type = EXCLUDED.load_type,
        batch_idx = EXCLUDED.batch_idx;
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


# отправка уведомлений о статусе тасок в Telegram
def send_telegram_notification(dag_run=None, dag=None, watched_tasks=None, **context):
    try:
        # Подтягиваем dag и dag_run из context, если не переданы явно
        dag_run = dag_run or context.get("dag_run")
        dag = dag or context.get("dag")
        
        if not dag_run or not dag or not watched_tasks:
            logging.warning("send_telegram_notification вызван без dag_run, dag или watched_tasks")
            return

        telegram_token = Variable.get('TG_BOT_TOKEN')
        chat_id = Variable.get('TG_BOT_CHAT_ID')

        task_results = []
        failed = False

        # Получаем статус каждой задачи из watched_tasks
        for task_id in watched_tasks:
            ti = dag_run.get_task_instance(task_id)
            state = ti.state if ti else "unknown"

            if state != "success":
                failed = True

            icon = "✅" if state == "success" else "❌"
            task_results.append(f"{icon} `{task_id}` — *{state.upper()}*")

        overall_status = "❌ *FAILED*" if failed else "✅ *SUCCESS*"
        severity = "CRITICAL" if failed else "INFO"

        run_time = dag_run.end_date or dag_run.execution_date
        run_time_str = run_time.strftime('%Y-%m-%d %H:%M:%S') if run_time else "N/A"

        # Формируем сообщение
        message = f"""
🔥 *Airflow Alert* 🔥

*DAG:* `{dag.dag_id}`
*Overall status:* {overall_status}
*Severity:* `{severity}`

*Tasks:*
{chr(10).join(task_results)}

*Run ID:* `{dag_run.run_id}`
🕒 {run_time_str}
        """

        # Отправка в Telegram
        url = f"https://api.telegram.org/bot{telegram_token}/sendMessage"
        resp = requests.post(url, json={
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "Markdown"
        })

        if resp.status_code != 200:
            logging.error(f"Ошибка отправки Telegram: {resp.text}")
        else:
            logging.info("Telegram уведомление отправлено")

    except Exception as e:
        logging.error(f"Не удалось отправить сообщение в Telegram: {e}")
