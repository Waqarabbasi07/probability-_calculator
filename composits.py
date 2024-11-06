import pandas as pd
import json
from collections import Counter

# Pcode CSV path 
csv_file_path = '/home/waqar/Downloads/geocoded_postcode_file.csv'
postcode_df = pd.read_csv(csv_file_path)

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

def fix_name_format(legal_name):
    try:
        # Split the name based on commas if present
        parts = legal_name.split(",")
        if len(parts) == 2:
            first_name = parts[1].strip()
            last_name = parts[0].strip()
            return f"{first_name.upper()} {last_name.upper()}"
        # If no commas, just return the name in uppercase
        return legal_name.upper()
    except Exception as e:
        print(f"Error in fix_name_format: {e}")
        return legal_name.upper()

# Main function to extract keys from the JSON sources
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

def process_source(source_name, source_data, keys_to_extract, final_output):
    try:
        postcode_keys = ["Post Code", "Post code", "PostalCode", "Post_code", "Post_Code", "pcode", "Postal_Code", "Postal_code"]
        
        for final_key, possible_keys in keys_to_extract.items():
            for key in possible_keys:
                if key in source_data:
                    value = source_data[key]
                    # Skip SUbrub if the source is tpbData
                    if final_key =="Suburb" and source_name == "tpbData":
                        continue
                    # Skip  ABN 
                    if final_key == "ABN" and source_name == "tpbData":
                        continue

                    if final_key == "State":
                        value = convert_state_to_abbreviation(value)
                    if final_key == "Legal Name":
                        value = fix_name_format(value)
                    
                    add_to_output(final_key, value, source_name, final_output)


        # if NOT tpbData and other postcode, match it with the CSV filestcode
        if source_name != "tpbData":
            for key in postcode_keys:

                if key in source_data:
                    pcode = source_data.get(key)
                    
                    if pcode:
                        pcode = int(pcode)                       
                        locality_row = postcode_df[postcode_df['Pcode'] == pcode]
                        if not locality_row.empty:
                            locality = locality_row['Locality'].values[0]
                            
                            add_to_output("Locality", locality, source_name, final_output)
                    break

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
            "Suburb": ["Suburb","suburb"],
            "Entity Name": ["Entity Name","EntityName"],
            "Trading name": ["Trading Name","TradingName"],
            "Name": ["Name"],
            "Company Name": ["company_name","Company_Name","Company Name","CompanyName"],
            "Business Name": ["business_name", "Business name","BusinessName","Business Name"],
            "ABR Entity Type": ["Entity Type"],
            "ABN": ["ABN"],
            "Suburb":["Suburb","suburb"],
            "LegalName":["LegalName", "legal_name"],
            "ACN": ["ASIC Number", "RegistrationAuthorityEntityID"],
            "Postal code": ["Post Code", "Post code", "PostalCode", "Post_code", "Post_Code", "pcode", "Postal_Code", "Postal_code"],
            "State": ["State", "Region"],
            "Active Status": ["Entity Status Code", "EntityStatus"],
            "ABR Last Updated Date": ["recordLastConfirmedDate"],
            "GST Effective Date": ["Goods And Services Tax"],
            "ABR Last Confirmed Date": ["recordLastConfirmedDate"],
            "Locality": ["Locality"]
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
 
            action = ["ADD", "DELETE", "EDIT"] if key in ["State", "Postal code"] else ["DELETE"]
            if len(value_counts) == 1:
                count = value_counts[items[0]["value"]]
                for item in items:
                    score = "High" if count > 1 else "Medium"
                    result.setdefault(key, []).append({
                        "value": item["value"],
                        "source": item["source"],
                        "score": score,
                        "action": action
                    })

            elif len(value_counts) == total_items:
                for item in items:
                    result.setdefault(key, []).append({
                        "value": item["value"],
                        "source": item["source"],
                        "score": "Low",
                        "action": action
                    })

            else:
                frequencies = sorted(value_counts.values(), reverse=True)
                max_count = frequencies[0]

                for item in items:
                    value = item["value"]
                    count = value_counts[value]

                    if count == max_count and frequencies.count(max_count) == 1:
                        score = "High"
                    elif count > 1:
                        score = "Medium"
                    else:
                        score = "Low"

                    result.setdefault(key, []).append({
                        "value": value,
                        "source": item["source"],
                        "score": score,
                        "action": action
                    })

            result[key] = sorted(
                result[key], key=lambda x: score_order[x["score"]], reverse=True
            )

        ordered_result = {}
        priority_keys = ["LegalName", "Entity Name", "Business Name"]
      
        for key in priority_keys:
            if key in result:
                ordered_result[key] = result.pop(key)
        
        for key in sorted(result.keys()):
            if "Name" in key and key not in priority_keys:
                ordered_result[key] = result.pop(key)

        ordered_result.update(result)

    except Exception as e:
        print(f"An error occurred: {e}")
    return ordered_result


def process_json_input(json_string):
    extracted_data = extract_keys_from_sources(json_string)
    updated_data = set_dynamic_probability(extracted_data)
    return updated_data


json_input = """{
    "combinedResults": {
        "abrData": {
            "ABN": "58453256019",
            "Entity Status Code": "Active",
            "Entity Type": "IND",
            "Entity Description": "Individual/Sole Trader",
            "Goods And Services Tax": "2016-04-01",
            "State": "QLD",
            "Post Code": "4380",
            "Probable Locality": "AMIENS;AMOSFIELD;BROADWATER;CANNON CREEK;DALCOUTH;DIAMONDVALE;EUKEY;GLENLYON;GREENLANDS;KYOOMBA;MINGOOLA;MINGOOLA;MOUNT TULLY;NUNDUBBERMERE;PIKEDALE;PIKES CREEK;RUBY CREEK;SEVERNLEA;SPRINGDALE;STANTHORPE;STORM KING;SUGARLOAF;THORNDALE;UNDERCLIFFE",
            "Entity Name": "BICKERTON, KELLIE ",
            "Business Name": "kbbk. solutions;BY ALL ACCOUNTS BOOKKEEPING",
            "Trading Name": "BY ALL ACCOUNTS BOOKKEEPING",
            "recordLastConfirmedDate": "2024-09-25T07:06:36.396Z",
            "Suburb": "STANTHORPE"
        },

        "acnData": [],
        "tpbData": {
            "nid": "150651",
            "legal_name": "Kellie Bickerton",
            "business_name": "KBBK Solutions",
            "registration_number": "25430469",
            "practitioner_type": "BAS Agent",
            "registration_status": "Registered",
            "Suburb": "STANTHORPE",
            "state": "Queensland",
            "ran": "25430469"
        },
        "quickbookData": [
            {
                "_id": "66cd7f71c2cbde975fddf317",
                "company_name": "KBBK Solutions",
                "pcode": "4380",
                "state": "QLD",
                "scrapping_date": "07-August-2024",
                "persons": [
                    {
                        "source_url": "https://proadvisor.intuit.com/app/accountant/search?searchId=kellee-bickerton73",
                        "name": "Kellee Bickerton",
                        "location": "PO Box 1004\\nStanthorpe, QLD 4380",
                        "website_info": null,
                        "phone": null,
                        "about": "I love numbers, they don't argue!  \\n\\nAs an ASIC and BAS Agent my goal is to transform your business and its figures into something you can understand and work with.\\n\\nUnfortunately my books are closed right now.  Good luck searching for a new Agent.",
                        "services": "Book cleanup, Bookkeeping, Payroll, QuickBooks Payroll, QuickBooks setup, 1 moreQuickBooks training",
                        "years_in_business": "27",
                        "industries_served": "Agriculture, Farming, Engineering, Financial Service, Healthcare, Hotel, Leisure, Hospitality, Infrastructure, Not For Profit, Professional Services, Retail, Trades, Construction, Transport",
                        "languages": null,
                        "social_sites": null,
                        "credentials": "Bookkeeper",
                        "software_expertise": "QuickBooks Online (incl. payroll)",
                        "more_information": "Find a ProAdvisor\\n\\n\\nStill exploring?See more\\nOFFERS FREE CONSULTATION\\nAdvanced ProAdvisor\\nKellee Bickerton\\nNo reviews yet\\nKBBK Solutions\\nPO Box 1004\\nStanthorpe, QLD 4380\\nPhone number(s)\\nServices\\nBook cleanup\\nBookkeeping\\nPayroll\\nQuickBooks Payroll\\nQuickBooks setup\\nQuickBooks training\\nShow less\\nCertified expert in\\nQuickBooks\\nOnline (ADVANCED)\\nAbout me\\nI love numbers, they don't argue!  \\n\\nAs an ASIC and BAS Agent my goal is to transform your business and its figures into something you can understand and work with.\\n\\nUnfortunately my books are closed right now.  Good luck searching for a new Agent.\\nYears in business\\n27\\nIndustries served\\nAgriculture / Farming\\nEngineering\\nFinancial Service\\nHealthcare\\nHotel / Leisure / Hospitality\\nInfrastructure\\nNot For Profit\\nProfessional Services\\nRetail\\nTrades / Construction\\nTransport\\nLanguages\\nSocial sites\\nCredentials\\n†\\nBookkeeper\\nSoftware expertise\\nQuickBooks Online (incl. payroll)\\nClient reviews\\nWrite a review\\nNo clients have provided a review yet\\nSend a messageMore info\\nYour name\\nYour email\\nYour phone numberoptional\\nSend message\\n© 2020 Intuit Inc. All rights reserved\\nPrivacy statement\\n|"
                    }
                ]
            }
        ],
        "xeroData": [],
        "leiData": [],
        "caData": [],
        "paData": [],
        "asxData": []
    }
}
"""

response = process_json_input(json_input)
print(json.dumps(response, indent=4))