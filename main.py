import asyncio
import json, os
from ensurepip import bootstrap
from math import floor

from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from openai import OpenAI
from kafka import KafkaProducer
from dotenv import load_dotenv

# Load the environment variables from .env file
load_dotenv()

# Retrieve environment variables
API_KEY = os.getenv("OPEN_AI_API_KEY")
SBR_WS_CDP = os.getenv("SBR_WS_CDP")
BASE_URL = os.getenv("BASE_URL")
LOCATION = os.getenv("LOCATION")

# Initialize the OpenAI client using the API key from the environment
client = OpenAI(api_key=API_KEY)

data = {}

def extract_picture(picture_section):
    soup = BeautifulSoup(picture_section, 'html.parser')
    picture_tags = soup.find_all('picture', class_='_1rk40754')

    # Extract image URLs
    image_urls = []
    for picture in picture_tags:
        sources = picture.find_all('source')
        for source in sources:
            srcset = source.get('srcset')
            if srcset:
                urls = srcset.split(',')
                for url in urls:
                    image_url = url.split(' ')[0].strip()
                    # Only add URLs containing '/1024/' in the link
                    if '/1024/' in image_url:
                        image_urls.append(image_url)

    return image_urls


async def extract_property_details(input_content):
    print("Extracting property information...")

    command = f"""
        You are a data extraction model. Extract the following details in JSON format:
        {{
            "price": "",
            "address": "",
            "bedrooms": "",
            "bathrooms": "",
            "receptions": "",
            "EPC Rating":"",
            "tenure": "",
            "time_remaining_on_lease": "",
            "service_charge": "",
            "council_tax_band": "",
            "ground_rent": ""
        }}
        Here is the property details HTML:
        {input_content}
    """
    try:
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {
                    "role": "user",
                    "content": command
                }
            ]
        )
        res = response.choices[0].message.content
        json_data = json.loads(res)
        return json_data
    except Exception as e:
        data.update({"data_extracted": "partially"})
        print(f"Error extracting property details: {e}")
        return {}

async def run(pw, producer):
    print('Connecting to Scraping Browser...')
    browser = await pw.chromium.connect_over_cdp(SBR_WS_CDP)
    try:
        page = await browser.new_page()
        print(f'Connected! Navigating to {BASE_URL}')
        await page.goto(BASE_URL)

        await page.wait_for_selector('input[name="autosuggest-input"]', timeout=1200000)
        await page.fill('input[name="autosuggest-input"]', LOCATION)
        await page.keyboard.press('Enter')
        print("Waiting for search results")
        await page.wait_for_load_state("load")
        await page.wait_for_selector('div[data-testid="regular-listings"]', timeout=120000000)

        content = await page.inner_html('div[data-testid="regular-listings"]')
        soup = BeautifulSoup(content, 'html.parser')

        for idx, div in enumerate(soup.find_all('div', class_='dkr2t83')):
            link = div.find('a')['href']
            data.update({
                "address": div.find('address').text.strip() if div.find('address') else 'N/A',
                "title": div.find('h2').text.strip() if div.find('h2') else 'N/A',
                "link": BASE_URL + link,
                "data_extracted": "fully"
            })

            try:
                print(data['link'])
                await page.goto(data['link'])
                print("Navigating to listing page")
                await page.wait_for_selector("div._1rk40750", timeout=1200000)
                content_detail = await page.inner_html("div._1rk40750")
                print("Page loaded, extracting details...")
                pictures = extract_picture(content_detail)
                data['pictures'] = pictures
                # Extract additional property details using LLM
                property_details_html = await page.inner_html('div[class="_1olqsf96"]')
                property_details = await extract_property_details(property_details_html)
                data.update(property_details)
            except Exception as e:
                data.update({"data_extracted": "partially"})
                print(f"Error navigating to listing details: {e}")
                data['pictures'] = []
                data.update({})
            # html = await page.content()
            # print(html)
            print(data)
            print("SENDING DATA TO KAFKA")
            producer.send("properties", value=json.dumps(data).encode("utf-8"))
            print("Data send to kafka")
            break
    finally:
        await browser.close()

async def run_periodically(max_iterations=5):
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=5000)
    
    for iteration in range(max_iterations):
        print(f"Starting execution {iteration + 1} of {max_iterations}")
        try:
            async with async_playwright() as playwright:
                await run(playwright, producer)
        except Exception as e:
            print(f"Error during execution: {e}")
        
        # print("Execution completed. Waiting for 3 seconds before the next run.")
        # await asyncio.sleep(3)  # Wait for 3 seconds before running again

    print("Reached the maximum number of iterations. Stopping.")

if __name__ == '__main__':
    asyncio.run(run_periodically())

# async def main():
#     producer=KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=5000)
#     async with async_playwright() as playwright:
#         await run(playwright, producer)

# if __name__ == '__main__':
#     asyncio.run(main())