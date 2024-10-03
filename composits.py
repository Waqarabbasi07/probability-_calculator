import json
from collections import Counter

# State abbreviation conversion
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
    try:
        return state_full_to_abbreviation.get(state_value.lower(), state_value)
    except Exception as e:
        print(f"Error in convert_state_to_abbreviation: {e}")
        return state_value 


def fix_legal_name_format(legal_name):
    try:
        if "," in legal_name:
            parts = legal_name.split(",")
            if len(parts) == 2:
                first_name = parts[1].strip()
                last_name = parts[0].strip()
                return f"{first_name.upper()} {last_name.upper()}"
        return legal_name.upper()
    except Exception as e:
        print(f"Error in fix_legal_name_format: {e}")
        return legal_name.upper()

# Main function to extract keys from the JSON sources
import json

def add_to_output(final_key, value, source_name, final_output):
    try:
        value = str(value).upper()
        if final_key not in final_output:
            final_output[final_key] = []
        final_output[final_key].append({
            "value": value,
            "source": source_name
        })
    except Exception as e:
        print(f"Error in add_to_output: {e}")

def extract_name_keys(source_name, source_data, final_output):
    try:
        for key, value in source_data.items():
            if "Name" in key or "name" in key:
                if key == "legal_name" or key == "Entity Name" or key == "LegalName":
                    value = fix_legal_name_format(value)
                add_to_output("Legal Name", value, source_name, final_output)
    except Exception as e:
        print(f"Error in extract_name_keys: {e}")

def process_state_value(state_value):
    try:
        return convert_state_to_abbreviation(state_value)
    except Exception as e:
        print(f"Error in process_state_value: {e}")
        return state_value  # Return the original state_value in case of error

def process_source(source_name, source_data, keys_to_extract, final_output):
    try:
        for final_key, possible_keys in keys_to_extract.items():
            for key in possible_keys:
                if key in source_data:
                    value = source_data[key]
                    if final_key == "State":
                        value = process_state_value(value)
                    if final_key == "Legal Name":
                        value = fix_legal_name_format(value)
                    add_to_output(final_key, value, source_name, final_output)
        extract_name_keys(source_name, source_data, final_output)
    except Exception as e:
        print(f"Error in process_source: {e}")

def extract_keys_from_sources(json_data):
    try:
        if isinstance(json_data, str):
            try:
                json_data = json.loads(json_data)
            except json.JSONDecodeError as e:
                print(f"JSON Decode Error: {e}")
                return {}

        keys_to_extract = {
            "Legal Name": ["legal_name", "Entity Name", "LegalName"],
            "ABR Entity Type": ["Entity Type"],
            "ABN": ["ABN"],
            "ACN": ["ASIC Number", "RegistrationAuthorityEntityID"],
            "Postal code": ["Post Code", "Post code", "PostalCode", "Post_code", "Post_Code", "pcode", "Postal_Code", "Postal_code"],
            "State": ["State", "Region"],
            "Active Status": ["Entity Status Code", "EntityStatus"],
            "ABR Last Updated Date": ["recordLastConfirmedDate"],
            "GST Effective Date": ["Goods And Services Tax"],
            "ABR Last Confirmed Date": ["recordLastConfirmedDate"],
        }

        final_output = {}

        if "combinedResults" in json_data:
            try:
                for source, source_data in json_data["combinedResults"].items():
                    if isinstance(source_data, dict):
                        process_source(source, source_data, keys_to_extract, final_output)
                    elif isinstance(source_data, list):
                        for item in source_data:
                            process_source(source, item, keys_to_extract, final_output)
            except Exception as e:
                print(f"Error processing combinedResults: {e}")

        return final_output

    except Exception as e:
        print(f"Error in extract_keys_from_sources: {e}")
        return {}


def set_dynamic_probability(data):
    result = {}
    score_order = {"High": 3, "Medium": 2, "Low": 1}
    try:
        for key, items in data.items():
            value_counts = Counter([item["value"] for item in items])
            total_items = len(items)
            
            # Determine the action logic based on the key
            action = ["ADD", "DELETE", "EDIT"] if key in ["State", "Postal code"] else ["DELETE"]
            if len(value_counts) == 1:
                
                count = value_counts[items[0]["value"]]
                for item in items:
                    score = "High" if count > 1 else "Medium"
                    result.setdefault(key, []).append({
                        "value": item["value"],
                        "source": item["source"],
                        "score": score,
                        "action": action  # Added here
                    })

            elif len(value_counts) == total_items:
                for item in items:
                    result.setdefault(key, []).append({
                        "value": item["value"],
                        "source": item["source"],
                        "score": "Low",
                        "action": action  # Added here
                    })

            else:
                frequencies = sorted(value_counts.values(), reverse=True)
                max_count = frequencies[0]

                for item in items:
                    value = item["value"]
                    count = value_counts[value]

                    if count == max_count and frequencies.count(max_count) == 1:
                        score = "High"
                        # probability = 0.6

                    elif count > 1:
                        score = "Medium"
                        # probability = 0.3

                    else:
                        score = "Low"
                        # probability = 0.1

                    result.setdefault(key, []).append({
                        "value": value,
                        "source": item["source"],
                        "score": score,
                        "action": action  # Added here
                    })

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

json_input = """
{
    "combinedResults": {
        "abrData": {
            "ABN": "88765383115",
            "Entity Status Code": "Active",
            "Entity Type": "IND",
            "Goods And Services Tax": "2000-07-01",
            "State": "NSW",
            "Post code": "2251",
            "Entity Name": "GRANTHAM, ANTHONY"
        },
        "tpbData": {
            "nid": "131316",
            "Entity Type": "IND",
            "legal_name": "Anthony Grantham",
            "business_name": "Tony Grantham & Assoc",
            "state": "New South Wales"
        }
    }
}
"""
response = process_json_input(json_input)
print(json.dumps(response, indent=4))