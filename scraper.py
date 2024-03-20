from requests_html import HTMLSession
import pandas as pd
import re
import os
from datetime import datetime
from tqdm import tqdm
import csv
from rich import print

now = datetime.now()


def get_totalpage(r):
    """
    Extracts the total number of pages from the response object.

    Args:
        r (requests.Response): The HTML response object.

    Returns:
        int: The total number of pages.
    """
    pgnum = []
    for html in r.html:
        pgnum.append(html)
    return len(pgnum)


def get_allshopurl(session, url):
    """
    Extracts a list of shop URLs from the given base URL.

    Args:
        session (requests_html.HTMLSession): The HTML session object.
        url (str): The base URL to scrape shop listings.

    Returns:
        list: A list of shop URLs.
    """
    all_shop_details = []
    with session.get(url) as r:
        total_page = get_totalpage(r)
        for page in range(1, total_page + 1):
            page_url = url + f"?page={page}"
            response = session.get(page_url)
            shop_items = response.html.find("tbody.bg-white > tr.bg-white")

            for item in shop_items:
                district_state_links = item.find(
                    "div.text-sm.text-gray-900 > a.text-brown-600"
                )
                # Ensure at least one link is found, else default to Unknown
                district, state = "Unknown", "Unknown"
                if len(district_state_links) >= 1:
                    district = district_state_links[0].text
                    if len(district_state_links) >= 2:
                        state = district_state_links[1].text
                shop_link_element = item.find(
                    "div.flex.items-center > div.ml-4 > div > a", first=True
                )
                if shop_link_element:
                    shoplink = list(shop_link_element.absolute_links)[0]
                    all_shop_details.append((shoplink, district, state))
    return all_shop_details


def extract_shop_data(response):
    """
    Extracts shop data from a response object.

    Args:
        response (requests.Response): The HTML response object.

    Returns:
        dict: Extracted shop details including UID, shop name, and tags.
    """
    shop_data = {}

    uid = response.url.split("/")[3]
    shopname_element = response.html.find(
        "div.mt-5.text-center.text-2xl.font-bold.leading-9.tracking-tight.text-gray-900.flex.items-center.space-x-2.justify-center"
    )[0]
    shopname = shopname_element.find("span")[0].text
    tag_elements = response.html.find(
        "div.flex.space-x-2.justify-center.flex-wrap.-mt-2 a span"
    )
    tags = ", ".join([re.sub(r"[^\w\s]", "", element.text) for element in tag_elements])

    shop_data["Shop_ID"] = uid
    shop_data["Shopname"] = shopname
    shop_data["Tag"] = tags

    return shop_data


def extract_social_data(response):
    """
    Extracts social media links and location data from a response object.

    Args:
        response (requests.Response): The HTML response object.

    Returns:
        dict: Extracted social media and location details.
    """
    social_data = {}

    elements_mapping = {
        "googlemap_link": "div.flex.flex-col.space-y-4 a[href*='google.com']",
        "instagram_link": "div.flex.flex-col.space-y-4 a[href*='instagram.com']",
        "facebook_link": "div.flex.flex-col.space-y-4 a[href*='facebook.com']",
        "twitter_link": "div.flex.flex-col.space-y-4 a[href*='twitter.com']",
        "tiktok_link": "div.flex.flex-col.space-y-4 a[href*='tiktok.com']",
        "whatsapp_link": "div.flex.flex-col.space-y-4 a[href*='wa.me']",
    }

    for key, value in elements_mapping.items():
        element = response.html.find(value)
        if element:
            link = element[0].attrs["href"]
            if key == "googlemap_link":
                latitude = (
                    re.search(r"([0-9.]+),([0-9.]+)", link).group(1) if link else ""
                )
                longitude = (
                    re.search(r"([0-9.]+),([0-9.]+)", link).group(2) if link else ""
                )
                social_data["Latitude"] = latitude
                social_data["Longitude"] = longitude
            social_data[key.capitalize()] = link
        else:
            if key == "googlemap_link":
                social_data["Latitude"] = ""
                social_data["Longitude"] = ""
            social_data[key.capitalize()] = ""

    return social_data


def get_data(session, url, data):
    code_map = {
        "Johor": 1,
        "Kedah": 2,
        "Kelantan": 3,
        "Melaka": 4,
        "Negeri Sembilan": 5,
        "Pahang": 6,
        "Perak": 8,
        "Perlis": 9,
        "Pulau Pinang": 7,
        "Sabah": 12,
        "Sarawak": 13,
        "Selangor": 10,
        "Terengganu": 11,
        "Kuala Lumpur": 14,
        "Labuan": 15,
        "Putrajaya": 16,
    }
    shop_details = get_allshopurl(session, url)
    for details in tqdm(shop_details):
        link, district, state = details
        response = session.get(link)
        shop_data = extract_shop_data(response)
        social_data = extract_social_data(response)

        shop_data["District"] = district
        shop_data["State"] = state

        state_code = code_map.get(state, "Unknown")
        shop_data["State_code"] = state_code

        for key, value in {**shop_data, **social_data}.items():
            data[key].append(value)

        # Append date and time collected
        data["Date Collected"].append(now.strftime("%d-%m-%Y"))
        data["Time Collected"].append(now.strftime("%H:%M:%S"))


def clean_field(field):
    """Cleans the field by replacing or removing problematic characters."""
    if isinstance(field, str):
        return field.replace("\n", " ").replace("\r", " ").strip()
    return field


def main():
    session = HTMLSession()
    url = "https://petakopi.my/"

    data = {
        "Shop_ID": [],
        "Shopname": [],
        "Tag": [],
        "Googlemap_link": [],
        "District": [],
        "State": [],
        "State_code": [],
        "Latitude": [],
        "Longitude": [],
        "Instagram_link": [],
        "Facebook_link": [],
        "Twitter_link": [],
        "Tiktok_link": [],
        "Whatsapp_link": [],
        "Date Collected": [],
        "Time Collected": [],
    }

    get_data(session, url, data)

    file_name = "/code/output_csv/scraped_data.csv"
    existing_shop_ids = set()

    # Check if the file exists and read existing shop IDs to avoid duplications
    if os.path.exists(file_name):
        with open(file_name, mode="r", newline="", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            existing_shop_ids = {row["Shop_ID"] for row in reader}

    # Prepare new data, cleaning each field
    new_data_cleaned = {
        key: [clean_field(value) for value in values] for key, values in data.items()
    }

    # Append new data if not already exists
    with open(file_name, mode="a", newline="", encoding="utf-8") as file:

        # Ensure the file ends with a newline if it's not empty
        file.seek(0, os.SEEK_END)  # Move to the end of the file
        if file.tell() > 0:  # If the file is not empty
            file.write("\n")

        fieldnames = list(data.keys())  # Headers for CSV
        writer = csv.DictWriter(file, fieldnames=fieldnames)

        # Write headers if the file is new or empty
        if file.tell() == 0:
            writer.writeheader()

        # Append each new row to the CSV
        for i in range(len(new_data_cleaned["Shop_ID"])):
            row = {key: new_data_cleaned[key][i] for key in new_data_cleaned}
            if row["Shop_ID"] not in existing_shop_ids:
                print(f'New Value : {row["Shop_ID"]}')
                writer.writerow(row)

    print("Data appended successfully.")


if __name__ == "__main__":
    main()
