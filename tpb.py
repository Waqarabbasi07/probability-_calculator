from fastapi import FastAPI, HTTPException
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
import time
from bs4 import BeautifulSoup
import re

app = FastAPI()

def get_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920x1080")
    return webdriver.Chrome(options=chrome_options)

@app.get("/search/")
async def search_public_register(abn: str = None, name: str = None):
    if not abn and not name:
        raise HTTPException(status_code=400, detail="Either ABN or Name must be provided.")

    try:
        driver = get_driver()
        url = 'https://myprofile.tpb.gov.au/public-register/'
        driver.get(url)
        time.sleep(2)

        if abn:
            input_field = driver.find_element(By.ID, '1')
            input_field.send_keys(abn)
            input_field.send_keys(Keys.RETURN)
            find_button = driver.find_element(By.XPATH, '/html/body/div[5]/div/div/div[2]/div[1]/div/div/button[1]')
            find_button.click()
        elif name:
            input_field = driver.find_element(By.ID, '0')
            input_field.send_keys(name.title())
            find_button = driver.find_element(By.XPATH, '/html/body/div[5]/div/div/div[2]/div[1]/div/div/button[1]')
            find_button.click()
        else:
            driver.quit()
            raise HTTPException(status_code=400, detail="Invalid input for ABN or Name.")

        time.sleep(2)

        element = driver.find_element(By.XPATH, '/html/body/div[5]/div/div/div[2]/div[2]/div[1]')
        html_content = element.get_attribute('outerHTML')
        soup = BeautifulSoup(html_content, 'html.parser')

        tbody = soup.find('tbody')
        rows = tbody.find_all('tr') if tbody else []

        if not rows:
            driver.quit()
            return {"data": [], "message": "No records found"}

        data = []
        for row in rows:
            tds = row.find_all('td')
            if len(tds) >= 8:
                legal_name = tds[0].text.strip()
                business_name = tds[1].text.strip()
                trading_name = tds[1].text.strip()
                business_type = tds[3].text.strip()
                abn_value = tds[5].text.strip()
                suburb = tds[6].text.strip()
                state = tds[7].text.strip()

                last_td = tds[-1]
                business_address = last_td.get('data-value', '').strip()

                postal_code_match = re.search(r'\b\d{4}\b', business_address)
                postal_code = postal_code_match.group(0) if postal_code_match else ""

                structured_row_data = {
                    "Legal name": legal_name,
                    "Business name": business_name,
                    "Trading name": trading_name,
                    "Type": business_type,
                    "ABN": abn_value,
                    "Suburb": suburb,
                    "State": state,
                    "Business address": business_address,
                    "Postal code": postal_code
                }

                filtered_row_data = {k: v for k, v in structured_row_data.items() if v}
                data.append(filtered_row_data)

        driver.quit()
        return {"data": data, "message": "Records found"}

    except Exception as e:
        driver.quit()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
