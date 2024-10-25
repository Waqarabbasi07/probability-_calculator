from fastapi import FastAPI, Query
import requests
import json

app = FastAPI()

def get_lei_records(legal_name="", city="", postal_code=""):
    base_url = "https://api.gleif.org/api/v1/lei-records"
    params = {
        "filter[entity.legalName]": legal_name,
        "filter[entity.addresses.city]": city,
        "filter[entity.addresses.postalCode]": postal_code,
        "page[number]": 1,
        "page[size]": 50
    }
    params = {key: value for key, value in params.items() if value}
    response = requests.get(base_url, params=params)
    
    if response.status_code == 200:
        records = response.json().get('data', [])
        mapped_records = []
        for record in records:
            attributes = record.get('attributes', {})
            entity = attributes.get('entity', {})
            registration = attributes.get('registration', {})

            other_entity_names = entity.get('otherEntityNames', {})
            if isinstance(other_entity_names, dict):
                other_entity_names_list = other_entity_names.get('OtherEntityName', [""])
            else:
                other_entity_names_list = [""]

            other_addresses = entity.get('otherAddresses', {})
            if isinstance(other_addresses, dict):
                other_addresses_list = other_addresses.get('OtherAddress', [{"type": "", "lang": "", "FirstAddressLine": "", "City": "", "Country": "", "PostalCode": ""}])
            else:
                other_addresses_list = [{"type": "", "lang": "", "FirstAddressLine": "", "City": "", "Country": "", "PostalCode": ""}]

            mapped_record = {
                "_id": record.get("id", ""),
                "LEI": attributes.get("lei", ""),
                "Entity": {
                    "LegalName": entity.get("legalName", ""),
                    "OtherEntityNames": {
                        "OtherEntityName": other_entity_names_list
                    },
                    "LegalAddress": {
                        "lang": entity.get('legalAddress', {}).get("language", ""),
                        "FirstAddressLine": entity.get('legalAddress', {}).get("addressLines", ""),
                        "City": entity.get('legalAddress', {}).get("city", ""),
                        "Country": entity.get('legalAddress', {}).get("country", ""),
                        "PostalCode": entity.get('legalAddress', {}).get("postalCode", "")
                    },
                    "HeadquartersAddress": {
                        "lang": entity.get('headquartersAddress', {}).get("language", ""),
                        "FirstAddressLine": entity.get('headquartersAddress', {}).get("addressLines", ""),
                        "City": entity.get('headquartersAddress', {}).get("city", ""),
                        "Country": entity.get('headquartersAddress', {}).get("country", ""),
                        "PostalCode": entity.get('headquartersAddress', {}).get("postalCode", "")
                    },
                    "OtherAddresses": {
                        "OtherAddress": other_addresses_list
                    },
                    "RegistrationAuthority": {
                        "RegistrationAuthorityID": entity.get("registrationAuthority", {}).get("RegistrationAuthorityID", ""),
                        "RegistrationAuthorityEntityID": entity.get("registrationAuthority", {}).get("RegistrationAuthorityEntityID", "")
                    },
                    "LegalJurisdiction": entity.get("legalJurisdiction", ""),
                    "EntityCategory": entity.get("entityCategory", ""),
                    "LegalForm": {
                        "EntityLegalFormCode": entity.get("legalForm", {}).get("EntityLegalFormCode", "")
                    },
                    "EntityStatus": entity.get("entityStatus", ""),
                    "EntityCreationDate": entity.get("entityCreationDate", "")
                },
                "Registration": {
                    "InitialRegistrationDate": registration.get("initialRegistrationDate", ""),
                    "LastUpdateDate": registration.get("lastUpdateDate", ""),
                    "RegistrationStatus": registration.get("registrationStatus", ""),
                    "NextRenewalDate": registration.get("nextRenewalDate", ""),
                    "ManagingLOU": registration.get("managingLou", ""),
                    "ValidationSources": registration.get("validationSources", ""),
                    "ValidationAuthority": {
                        "ValidationAuthorityID": registration.get("validationAuthority", {}).get("ValidationAuthorityID", ""),
                        "ValidationAuthorityEntityID": registration.get("validationAuthority", {}).get("ValidationAuthorityEntityID", "")
                    }
                }
            }
            mapped_records.append(mapped_record)
        return mapped_records
    else:
        return {"error": f"Error: {response.status_code}"}

@app.get("/lei-records/")
def lei_records(legal_name: str = Query(default="", title="Legal Name"), 
                city: str = Query(default="", title="City"), 
                postal_code: str = Query(default="", title="Postal Code")):
    return get_lei_records(legal_name, city, postal_code)
