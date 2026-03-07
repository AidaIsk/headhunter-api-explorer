import xml.etree.ElementTree as ET
import pandas as pd

def parse_unsc_xml(xml_path):

    tree = ET.parse(xml_path)
    root = tree.getroot()

    records = []

    # INDIVIDUAL
    for person in root.findall(".//INDIVIDUAL"):

        first = person.findtext("FIRST_NAME")
        second = person.findtext("SECOND_NAME")

        name = " ".join(filter(None,[first,second]))

        records.append({
            "name": name.lower(),
            "type": "individual",
            "source": "UNSC"
        })

    # ENTITY
    for entity in root.findall(".//ENTITY"):

        name = entity.findtext("NAME")

        records.append({
            "name": name.lower(),
            "type": "organization",
            "source": "UNSC"
        })

    return pd.DataFrame(records)