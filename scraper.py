from requests_html import HTMLSession
import pandas as pd
import re
from datetime import datetime
from tqdm import tqdm

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

            # for item in shop_items:
            #     district_state_links = item.find(
            #         "div.text-sm.text-gray-900 > a.text-brown-600"
            #     )
            #     if len(district_state_links) >= 2:
            #         district = district_state_links[0].text
            #         state = district_state_links[1].text
            #     else:
            #         # Fallback values if not found
            #         district = "Unknown"
            #         state = "Unknown"
            #     shop_link_element = item.find(
            #         "div.flex.items-center > div.ml-4 > div > a", first=True
            #     )
            #     if shop_link_element:
            #         # shoplink = shop_link_element.attrs["href"]
            #         shoplink = list(shop_link_element.absolute_links)[0]
            #         all_shop_details.append((shoplink, district, state))
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
        "googlemap": "div.flex.flex-col.space-y-4 a[href*='google.com']",
        "instagram": "div.flex.flex-col.space-y-4 a[href*='instagram.com']",
        "facebook": "div.flex.flex-col.space-y-4 a[href*='facebook.com']",
        "twitter": "div.flex.flex-col.space-y-4 a[href*='twitter.com']",
        "tiktok": "div.flex.flex-col.space-y-4 a[href*='tiktok.com']",
        "whatsapp": "div.flex.flex-col.space-y-4 a[href*='wa.me']",
    }

    for key, value in elements_mapping.items():
        element = response.html.find(value)
        if element:
            link = element[0].attrs["href"]
            if key == "googlemap":
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
            if key == "googlemap":
                social_data["Latitude"] = ""
                social_data["Longitude"] = ""
            social_data[key.capitalize()] = ""

    return social_data


def get_data(session, url, data):
    shop_details = get_allshopurl(session, url)
    for details in tqdm(shop_details):
        link, district, state = details
        response = session.get(link)
        shop_data = extract_shop_data(response)
        social_data = extract_social_data(response)

        shop_data["District"] = district
        shop_data["State"] = state

        for key, value in {**shop_data, **social_data}.items():
            data[key].append(value)

        # Append date and time collected
        data["Date Collected"].append(now.strftime("%d-%m-%Y"))
        data["Time Collected"].append(now.strftime("%H:%M:%S"))


def main():
    session = HTMLSession()
    url = "https://petakopi.my/"

    data = {
        "Shop_ID": [],
        "Shopname": [],
        "Tag": [],
        "Googlemap": [],
        "District": [],
        "State": [],
        "Latitude": [],
        "Longitude": [],
        "Instagram": [],
        "Facebook": [],
        "Twitter": [],
        "Tiktok": [],
        "Whatsapp": [],
        "Date Collected": [],
        "Time Collected": [],
    }

    get_data(session, url, data)

    df = pd.DataFrame.from_dict(data, orient="index").transpose()
    df.to_csv(
        f'scraped_data_{now.strftime("%d-%m-%Y")}.csv',
        index=False,
    )
    print(f'CSV EXPORTED! - {now.strftime("%H:%M:%S")}')


if __name__ == "__main__":
    main()
