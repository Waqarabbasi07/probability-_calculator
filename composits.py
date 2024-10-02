from fastapi import FastAPI
from pydantic import BaseModel
from typing import Dict, Any, List
import json
from collections import Counter

app = FastAPI()

state_full_to_abbreviation = {
    "australian capital territory": "ACT",
    "new south wales": "NSW",
    "northern territory": "NT",
    "queensland": "QLD",
    "south australia": "SA",
    "tasmania": "TAS",
    "victoria": "VIC",
    "western australia": "WA",
}

def convert_state_to_abbreviation(state_value):
    return state_full_to_abbreviation.get(state_value.lower(), state_value)

def fix_legal_name_format(legal_name):
    if "," in legal_name:
        parts = legal_name.split(",")
        if len(parts) == 2:
            first_name = parts[1].strip()
            last_name = parts[0].strip()
            return f"{first_name.upper()} {last_name.upper()}"
    return legal_name.upper()

def extract_keys_from_sources(json_data):
    if isinstance(json_data, str):
        try:
            json_data = json.loads(json_data)
        except json.JSONDecodeError as e:
            return {}

    keys_to_extract = {
        "Legal Name": ["legal_name", "Entity Name", "LegalName"],
        "ABR Entity Type": ["Entity Type"],
        "ABN": ["ABN"],
        "ACN": ["ASIC Number", "RegistrationAuthorityEntityID"],
        "Postal code": ["Post Code", "Post code", "PostalCode", "Post_code", "Post_Code", "pcode", ""],
        "State": ["State", "Region"],
        "Active Status": ["Entity Status Code", "EntityStatus"],
        "ABR Last Updated Date": ["recordLastConfirmedDate"],
        "GST Effective Date": ["Goods And Services Tax"],
        "ABR Last Confirmed Date": ["recordLastConfirmedDate"],
    }

    final_output = {}

    def add_to_output(final_key, value, source_name):
        value = str(value).upper()
        if final_key not in final_output:
            final_output[final_key] = []
        final_output[final_key].append({"value": value, "source": source_name})

    def extract_name_keys(source_name, source_data):
        for key, value in source_data.items():
            if "Name" in key or "name" in key:
                if key == "legal_name" or key == "Entity Name" or key == "LegalName":
                    value = fix_legal_name_format(value)
                add_to_output("Legal Name", value, source_name)

    def process_state_value(state_value):
        return convert_state_to_abbreviation(state_value)

    def process_source(source_name, source_data):
        for final_key, possible_keys in keys_to_extract.items():
            for key in possible_keys:
                if key in source_data:
                    value = source_data[key]
                    if final_key == "State":
                        value = process_state_value(value)
                    if final_key == "Legal Name":
                        value = fix_legal_name_format(value)
                    add_to_output(final_key, value, source_name)
        extract_name_keys(source_name, source_data)

    if "combinedResults" in json_data:
        for source, source_data in json_data["combinedResults"].items():
            if isinstance(source_data, dict):
                process_source(source, source_data)
            elif isinstance(source_data, list):
                for item in source_data:
                    process_source(source, item)

    return final_output

def set_dynamic_probability(data):
    result = {}
    score_order = {"High": 3, "Medium": 2, "Low": 1}
    try:
        for key, items in data.items():
            value_counts = Counter([item["value"] for item in items])
            total_items = len(items)

            if len(value_counts) == 1:
                count = value_counts[items[0]["value"]]
                for item in items:
                    score = "High" if count > 1 else "Medium"
                    result.setdefault(key, []).append(
                        {
                            "value": item["value"],
                            "source": item["source"],
                            "score": score
                        }
                    )

            elif len(value_counts) == total_items:
                for item in items:
                    result.setdefault(key, []).append(
                        {
                            "value": item["value"],
                            "source": item["source"],
                            "score": "Low"
                        }
                    )

            else:
                frequencies = sorted(value_counts.values(), reverse=True)
                max_count = frequencies[0]

                for item in items:
                    value = item["value"]
                    count = value_counts[value]

                    if count == max_count and frequencies.count(max_count) == 1:
                        score = "High"
                        probability = 0.6

                    elif count > 1:
                        score = "Medium"
                        probability = 0.3

                    else:
                        score = "Low"
                        probability = 0.1

                    result.setdefault(key, []).append(
                        {
                            "value": value,
                            "source": item["source"],
                            "score": score
                        }
                    )

            result[key] = sorted(
                result[key], key=lambda x: score_order[x["score"]], reverse=True
            )
    except Exception as e:
        print(f"An error occurred: {e}")
    return result

def process_json_input(json_string):
    extracted_data = extract_keys_from_sources(json_string)
    updated_data = set_dynamic_probability(extracted_data)
    return updated_data

class InputModel(BaseModel):
    combinedResults: Dict[str, Any]

@app.post("/process/")
def process_data(input_data: InputModel):
    json_string = input_data.dict()
    result = process_json_input(json_string)
    return result