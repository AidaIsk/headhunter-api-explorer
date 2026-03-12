from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import execute_values
from datetime import datetime
import hashlib


def generate_entity_hash(entity):

    text = (
        str(entity.get("name", "")) +
        str(entity.get("reference_number", "")) +
        str(entity.get("gender", ""))
    )

    return hashlib.sha256(text.encode()).hexdigest()


def load_unsc_to_silver(parsed_data):

    postgres = PostgresHook(postgres_conn_id="postgres_bronze")
    conn = postgres.get_conn()
    cursor = conn.cursor()

    now = datetime.utcnow()

    # ------------------------
    # ENTITIES
    # ------------------------

    entities_rows = []

    for e in parsed_data["entities"]:

        entity_hash = generate_entity_hash(e)

        entities_rows.append((
            e["entity_id"],
            e.get("version"),
            e.get("entity_type"),
            e.get("name"),
            e.get("reference_number"),
            e.get("listed_on"),
            e.get("gender"),
            e.get("comments"),
            entity_hash,
            now,
            None,
            True
        ))

    sql_entities = """
    INSERT INTO silver.sanctions_entities
    (
        dataid,
        version,
        entity_type,
        name,
        reference_number,
        listed_on,
        gender,
        comments,
        entity_hash,
        valid_from,
        valid_to,
        is_current
    )
    VALUES %s
    """

    execute_values(cursor, sql_entities, entities_rows)

    # ------------------------
    # ALIASES
    # ------------------------

    aliases_rows = []

    for a in parsed_data["aliases"]:

        aliases_rows.append((
            a["entity_id"],
            a["alias"],
            now,
            None,
            True
        ))

    sql_aliases = """
    INSERT INTO silver.sanctions_aliases
    (
        dataid,
        alias,
        valid_from,
        valid_to,
        is_current
    )
    VALUES %s
    """

    if aliases_rows:
        execute_values(cursor, sql_aliases, aliases_rows)

    # ------------------------
    # NATIONALITIES
    # ------------------------

    nat_rows = []

    for n in parsed_data["nationalities"]:

        nat_rows.append((
            n["entity_id"],
            n["nationality"],
            now,
            None,
            True
        ))

    sql_nat = """
    INSERT INTO silver.sanctions_nationalities
    (
        dataid,
        nationality,
        valid_from,
        valid_to,
        is_current
    )
    VALUES %s
    """

    if nat_rows:
        execute_values(cursor, sql_nat, nat_rows)

    # ------------------------
    # DOCUMENTS
    # ------------------------

    doc_rows = []

    for d in parsed_data["documents"]:

        doc_rows.append((
            d["entity_id"],
            d["doc_type"],
            d["doc_number"],
            now,
            None,
            True
        ))

    sql_docs = """
    INSERT INTO silver.sanctions_documents
    (
        dataid,
        doc_type,
        doc_number,
        valid_from,
        valid_to,
        is_current
    )
    VALUES %s
    """

    if doc_rows:
        execute_values(cursor, sql_docs, doc_rows)

    conn.commit()

    cursor.close()
    conn.close()