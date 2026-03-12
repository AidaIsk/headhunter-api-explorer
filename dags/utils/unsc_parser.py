import xml.etree.ElementTree as ET


def safe_text(value):
    """Безопасная очистка текста."""
    if value:
        return value.strip()
    return None

def normalize_date(date_str):

    if not date_str:
        return None

    # пример: 2015-04-07-04:00
    return date_str[:10]

def parse_unsc_xml(xml_string):

    root = ET.fromstring(xml_string)

    entities = []
    aliases = []
    nationalities = []
    documents = []

    # -------- INDIVIDUALS --------

    for person in root.findall(".//INDIVIDUAL"):

        entity_id = person.findtext("DATAID")
        version = person.findtext("VERSIONNUM")

        parts = [
            person.findtext("FIRST_NAME"),
            person.findtext("SECOND_NAME"),
            person.findtext("THIRD_NAME"),
            person.findtext("FOURTH_NAME"),
        ]

        name = " ".join([p.strip() for p in parts if p])

        reference_number = safe_text(person.findtext("REFERENCE_NUMBER"))
        listed_on = safe_text(normalize_date(person.findtext("LISTED_ON")))
        gender = safe_text(person.findtext("GENDER"))
        comments = safe_text(person.findtext("COMMENTS1"))

        entities.append({
            "entity_id": entity_id,
            "version": version,
            "entity_type": "INDIVIDUAL",
            "name": name,
            "reference_number": reference_number,
            "listed_on": listed_on,
            "gender": gender,
            "comments": comments
        })

        # --- aliases

        for alias in person.findall("INDIVIDUAL_ALIAS"):

            alias_name = safe_text(alias.findtext("ALIAS_NAME"))

            if alias_name:
                aliases.append({
                    "entity_id": entity_id,
                    "alias": alias_name
                })

        # --- nationalities

        for nat in person.findall("NATIONALITY"):

            value = safe_text(nat.findtext("VALUE"))

            if value:
                nationalities.append({
                    "entity_id": entity_id,
                    "nationality": value
                })

        # --- documents

        for doc in person.findall("INDIVIDUAL_DOCUMENT"):

            doc_type = safe_text(doc.findtext("TYPE_OF_DOCUMENT"))
            doc_number = safe_text(doc.findtext("NUMBER"))

            if doc_type or doc_number:
                documents.append({
                    "entity_id": entity_id,
                    "doc_type": doc_type,
                    "doc_number": doc_number
                })

    # -------- ENTITIES --------

    for entity in root.findall(".//ENTITY"):

        entity_id = entity.findtext("DATAID")
        version = entity.findtext("VERSIONNUM")

        name = safe_text(entity.findtext("FIRST_NAME"))

        reference_number = safe_text(entity.findtext("REFERENCE_NUMBER"))
        listed_on = safe_text(normalize_date(entity.findtext("LISTED_ON")))
        comments = safe_text(entity.findtext("COMMENTS1"))

        entities.append({
            "entity_id": entity_id,
            "version": version,
            "entity_type": "ENTITY",
            "name": name,
            "reference_number": reference_number,
            "listed_on": listed_on,
            "gender": None,
            "comments": comments
        })

        # --- aliases

        for alias in entity.findall("ENTITY_ALIAS"):

            alias_name = safe_text(alias.findtext("ALIAS_NAME"))

            if alias_name:
                aliases.append({
                    "entity_id": entity_id,
                    "alias": alias_name
                })

    return {
        "entities": entities,
        "aliases": aliases,
        "nationalities": nationalities,
        "documents": documents
    }