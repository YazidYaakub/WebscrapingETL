from requests_html import HTMLSession
import pandas as pd
import re
from datetime import datetime

now = datetime.now()


def get_totalpage(r):
    pgnum = []
    for html in r.html:
        pgnum.append(html)
    return len(pgnum)


def get_shopurl(url):
    session = HTMLSession()
    with session.get(url) as r:
        r.html.render(sleep=2)
        total_pages = get_totalpage(r)
        data = {
            "UID": [],
            "Shopname": [],
            "Tag": [],
            "Gmap": [],
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
        for page in range(1, total_pages + 1):
            page_url = url + f"?page={page}"
            response = session.get(page_url)
            shop_links = response.html.find(
                "div.text-sm.font-medium.text-gray-900.flex.items-center"
            )
            for shop_link in shop_links:
                shoplink = shop_link.absolute_links
                for link in shoplink:
                    response = session.get(link)

                    uid = link.split("/")[3]

                    shopname_element = response.html.find(
                        "div.mt-5.text-center.text-2xl.font-bold.leading-9.tracking-tight.text-gray-900.flex.items-center.space-x-2.justify-center"
                    )[0]

                    shopname = shopname_element.find("span")[0].text
                    tag_elements = response.html.find(
                        "div.flex.space-x-2.justify-center.flex-wrap.-mt-2 a span"
                    )
                    # tag = [element.text for element in tag_elements]
                    tag = ", ".join(
                        [
                            re.sub(r"[^\w\s]", "", element.text)
                            for element in tag_elements
                        ]
                    )

                    google_element = response.html.find(
                        "div.flex.flex-col.space-y-4 a[href*='google.com']"
                    )

                    instagram_element = response.html.find(
                        "div.flex.flex-col.space-y-4 a[href*='instagram.com']"
                    )

                    facebook_element = response.html.find(
                        "div.flex.flex-col.space-y-4 a[href*='facebook.com']"
                    )

                    twitter_element = response.html.find(
                        "div.flex.flex-col.space-y-4 a[href*='twitter.com']"
                    )

                    tiktok_element = response.html.find(
                        "div.flex.flex-col.space-y-4 a[href*='tiktok.com']"
                    )

                    whatsapp_element = response.html.find(
                        "div.flex.flex-col.space-y-4 a[href*='wa.me']"
                    )

                    googlemap = (
                        google_element[0].attrs["href"] if google_element else ""
                    )

                    latitude = (
                        re.search(r"([0-9.]+),([0-9.]+)", googlemap).group(1)
                        if googlemap
                        else ""
                    )

                    longitude = (
                        re.search(r"([0-9.]+),([0-9.]+)", googlemap).group(2)
                        if googlemap
                        else ""
                    )

                    instagram = (
                        instagram_element[0].attrs["href"] if instagram_element else ""
                    )
                    facebook = (
                        facebook_element[0].attrs["href"] if facebook_element else ""
                    )
                    twitter = (
                        twitter_element[0].attrs["href"] if twitter_element else ""
                    )
                    tiktok = tiktok_element[0].attrs["href"] if tiktok_element else ""
                    whatsapp = (
                        whatsapp_element[0].attrs["href"] if whatsapp_element else ""
                    )

                    data["UID"].append(uid)
                    data["Shopname"].append(shopname)
                    data["Tag"].append(tag)
                    data["Gmap"].append(googlemap)
                    data["Latitude"].append(latitude)
                    data["Longitude"].append(longitude)
                    data["Instagram"].append(instagram)
                    data["Facebook"].append(facebook)
                    data["Twitter"].append(twitter)
                    data["Tiktok"].append(tiktok)
                    data["Whatsapp"].append(whatsapp)
                    data["Date Collected"].append(now.strftime("%d-%m-%Y"))
                    data["Time Collected"].append(now.strftime("%H:%M:%S"))

    df = pd.DataFrame.from_dict(data, orient="index").transpose()
    df.to_csv(f'/output csv/pe:takopi_data_{now.strftime("%d-%m-%Y")}.csv', index=False)
    print(f'CSV EXPORTED! - {now.strftime("%H:%M:%S")}')


def main():
    get_shopurl("https://petakopi.my/")


if __name__ == "__main__":
    main()
