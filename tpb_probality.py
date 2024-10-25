from fastapi import FastAPI, HTTPException
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
import time
from bs4 import BeautifulSoup
import re
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

                    # Skip adding ABN if the source is tpbData
                    if final_key == "ABN" and source_name == "tpbData":
                        continue

                    if final_key == "State":
                        value = convert_state_to_abbreviation(value)
                    if final_key == "Legal Name":
                        value = fix_name_format(value)
                    
                    add_to_output(final_key, value, source_name, final_output)

        # if NOT tpbData and other postcode, match it with the CSV file
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
            "Suburb": ["Suburb"],
            "Entity Name": ["Entity Name","EntityName"],
            "Trading name": ["Trading Name",""],
            "Name": ["Name"],
            "Company Name": ["company_name","Company_Name","Company Name"],
            "Business name": ["business_name", "Business Name","BusinessName"],
            "Legal name": ["legal_name", "LegalName", "Legal name"],
            "ABR Entity Type": ["Entity Type"],
            "ABN": ["ABN"],
            "ACN": ["ASIC Number", "RegistrationAuthorityEntityID"],
            "Postal code": ["Post Code", "Post code", "PostalCode", "Post_code", "Post_Code", "pcode", "Postal_Code", "Postal_code"],
            "State": ["State", "Region"],
            "Active Status": ["Entity Status Code", "EntityStatus"],
            "ABR Last Updated Date": ["recordLastConfirmedDate"],
            "GST Effective Date": ["Goods And Services Tax"],
            "ABR Last Confirmed Date": ["recordLastConfirmedDate"],
            "Locality": ["Locality"],
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
        priority_keys = ["Legal Name", "Entity Name", "Business Name"]
      
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

json_input = json_input = """{
  "combinedResults": {
    "abrData": {
      "ABN": "91178504772",
      "EntityStatusCode": "Active",
      "EntityType": "IND",
      "Legal Name":"qqqqqqqqqqq",
      "EntityDescription": "Individual/Sole Trader",
      "GoodsAndServicesTax": "2000-07-01",
      "State": "SA",
      "PostCode": "5067",
      "ProbableLocality": "BEULAH PARK; KENT TOWN; NORWOOD; NORWOOD SOUTH; ROSE PARK",
      "EntityName": "SANDOW, ANDREW KENNETH",
      "BusinessName": "ANDREW SANDOW BUSINESS SERVICES",
      "TradingName": "ANDREW SANDOW BUSINESS SERVICES",
      "RecordLastConfirmedDate": "2024-09-25T07:09:05.128Z"
    },
    "acnData": {"ABN": "11111111111",
      "EntityStatusCode": "Active"},
    "tpbData": {"ABN": "111111111233333332222",
    "Locality":"ss",
    "pcode":"5067",
    "Suburb":"aaaaaaa",

      "EntityStatusCode": "Active"},
    "quickbookData": {"ABN": "11111111122222",
      "EntityStatusCode": "Active"},
    "xeroData": [
      {
        "_id": "66ed171ac018a7cdce281a47",
        "SourceURL": "https://www.xero.com/advisors/bookkeeper/andrew-sandow-business-services-8b5c7d4d0ad6/",
        "Name": "Andrew Sandow Business Services",
        "Address": "Bookkeeper · 68 Rundle Street, Unit 2, Kent Town, South Australia",
        "Phone": null,
        "ViewWebsite": null,
        "About": "Sole trader. In practice for over twenty five years. Formerly a successful Farmer",
        "BankConnections": "Bendigo Bank (AU), National Australia Bank NAB (AU)",
        "IndustriesServed": "Healthcare and social services, Real estate and hiring services",
        "FindUs": {
          "Location1": "Kent Town\\n68 Rundle Street, Unit 2, Kent Town, 5067, South Australia\\n+61 0419849296"
        },
        "MoreInformation": "Try Xero for free\\nLogin\\nFIND AN ADVISOR ANDREW SANDOW BUSINESS SERVICES\\nAndrew Sandow Business Services\\nBookkeeper · 68 Rundle Street, Unit 2, Kent Town, South Australia\\nSend message\\nPhone number\\nSpecialist\\nSingle Touch Payroll\\nPartner status\\nBronze champion partner\\nExperience\\nPartner since 2010\\nCertification\\nXero payroll certified\\nCertification\\nXero advisor certified\\nAbout us\\nSole trader. In practice for over twenty five years. Formerly a successful Farmer\\nAffiliations\\nInstitute of Certified Bookkeepers (AU)\\nMeet the team\\nAndrew Sandow Business Services advisors: 1 - Xero certifications: 1\\nTo become certified, advisors complete learning modules on Xero’s key features and pass a practical exam.\\nAndrew Sandow\\nBookkeeper/BAS Agent\\nShow\\nAndrew Sandow Business Services: Bank experience\\nClients of Andrew Sandow Business Services connect to a range of banks via Xero. Andrew Sandow Business Services specialises in working with some of those banks; they can help with setting up bank feeds, and are knowledgeable about accessing bank loans and streamlining payments through Xero.\\nBank connections\\nBendigo Bank (AU)\\nNational Australia Bank NAB (AU)\\nAndrew Sandow Business Services: Industry experience\\nAndrew Sandow Business Services works with businesses across a range of industries. They specialise in helping to choose apps that integrate with Xero to boost business performance for some of those industries.\\nIndustries Served\\nHealthcare and social services\\nReal estate and hiring services\\nFind us\\nAndrew Sandow Business Services has an office in 1 location\\nKent Town\\n68 Rundle Street, Unit 2, Kent Town, 5067, South Australia\\n+61 0419849296\\nNew to Xero? Andrew Sandow Business Services can help\\nXero is beautiful online accounting software for smaller businesses. Andrew Sandow Business Services can help make your switch as smooth as possible.\\nContact them to find out how they can help your business.\\nPARTNER SINCE 2010\\nPlease note, Xero doesn't guarantee the service of any one advisor\\nWe don’t review advisor listings and we don’t recommend or guarantee the service provided by any one advisor, so make sure you check them out yourself and get comfortable with them yourself.\\nXero certification means that some or all team members have completed courses and been assessed by Xero as proficient in the use of the Xero application.\\nXero specialisation means that some or all team members have completed several short learning modules to develop skills in that area.\\nXero doesn’t assess any individual advisor’s or firm’s proficiency as an accountant, bookkeeper or financial advisor. And Xero certified advisors and advisors with industry or bank specialisations are not employees of Xero.",
        "State": "SA",
        "PostalCode": 2518,
        "Locality":"ssdd"
      }
    ],
    "leiData": [],
    "caData": [],
    "paData": [],
    "asxData": []
  }
}
"""

response = process_json_input(json_input)
print(json.dumps(response, indent=4))
