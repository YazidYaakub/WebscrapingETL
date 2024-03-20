from sqlalchemy import create_engine, text, MetaData, Table, select, insert, and_
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

host = os.getenv("HOST")
port = int(os.getenv("PORT"))
db_name = os.getenv("DB")
user = os.getenv("USER")
password = os.getenv("PASSWORD")

db_connection_string = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"

engine = create_engine(db_connection_string)
Session = sessionmaker(bind=engine)
session = Session()
metadata = MetaData()
metadata.reflect(bind=engine)

# Load the CSV data
df = pd.read_csv("output_csv/scraped_data.csv")
df["Tag"].fillna("N/A", inplace=True)
# Normalize and prepare data for insertion
df["Tag"] = df["Tag"].str.split(",")
tags = set(tag.strip() for sublist in df["Tag"].dropna() for tag in sublist)

# Tables reference
states_table = Table("states", metadata, autoload_with=engine)
districts_table = Table("districts", metadata, autoload_with=engine)
coordinates_table = Table("coordinates", metadata, autoload_with=engine)
shops_table = Table("shops", metadata, autoload_with=engine)
tags_table = Table("tags", metadata, autoload_with=engine)
shop_tags_table = Table("shop_tags", metadata, autoload_with=engine)
social_media_links_table = Table("social_media_links", metadata, autoload_with=engine)
data_collection_table = Table("data_collection", metadata, autoload_with=engine)

# Insert tags if not exist
for tag in tags:
    tag_ins = (
        insert(tags_table)
        .values(tag_description=tag)
        .on_conflict_do_nothing(index_elements=["tag_description"])
    )
    session.execute(tag_ins)

session.commit()

# Iterate through each row in the dataframe
for _, row in df.iterrows():
    # Check if the shop_id already exists in the database
    existing_shop_id = (
        session.query(shops_table)
        .filter(shops_table.c.shop_id == row["Shop_ID"])
        .first()
    )

    # If the shop_id does not exist, proceed with data ingestion
    if not existing_shop_id:
        print(existing_shop_id)
        # Insert or ignore logic for states and districts
        state_ins = (
            insert(states_table)
            .values(state_code=row["State_code"], state_name=row["State"])
            .on_conflict_do_nothing(index_elements=["state_code"])
        )
        district_ins = (
            insert(districts_table)
            .values(district_name=row["District"])
            .on_conflict_do_nothing(index_elements=["district_name"])
        )
        session.execute(state_ins)
        session.execute(district_ins)

        # Insert coordinates
        coord_id = session.execute(
            insert(coordinates_table)
            .values(latitude=row["Latitude"], longitude=row["Longitude"])
            .returning(coordinates_table.c.coordinate_id)
        ).fetchone()[0]

        # Insert shop
        shop_id = row["Shop_ID"]
        shop_ins = insert(shops_table).values(
            shop_id=shop_id,
            shopname=row["Shopname"],
            district_id=select(districts_table.c.district_id)
            .where(districts_table.c.district_name == row["District"])
            .scalar_subquery(),
            state_code=row["State_code"],
            coordinate_id=coord_id,
            googlemap_link=row["Googlemap_link"],
        )
        session.execute(shop_ins)

        # Insert shop tags
        # if row["Tag"] and not pd.isna(row["Tag"]):
        #     for tag in row["Tag"]:
        #         tag = tag.strip()
        #         if tag:
        #             tag_id = session.execute(
        #                 select(tags_table.c.tag_id).where(
        #                     tags_table.c.tag_description == tag.strip()
        #                 )
        #             ).fetchone()[0]
        #             shop_tag_ins = insert(shop_tags_table).values(
        #                 shop_id=shop_id, tag_id=tag_id
        #             )
        #             session.execute(shop_tag_ins)

        for tag in row["Tag"]:
            tag = tag.strip()

            tag_id = session.execute(
                select(tags_table.c.tag_id).where(tags_table.c.tag_description == tag)
            ).scalar()

            shop_tag_ins = insert(shop_tags_table).values(
                shop_id=shop_id, tag_id=tag_id
            )
            session.execute(shop_tag_ins)

        # Insert social media links
        social_media_ins = insert(social_media_links_table).values(
            shop_id=shop_id,
            instagram_link=row["Instagram_link"],
            facebook_link=row["Facebook_link"],
            twitter_link=row["Twitter_link"],
            tiktok_link=row["Tiktok_link"],
            whatsapp_link=row["Whatsapp_link"],
        )
        session.execute(social_media_ins)

        # Insert data collection
        datetime_collected = datetime.strptime(
            f"{row['Date Collected']} {row['Time Collected']}", "%d-%m-%Y %H:%M:%S"
        )
        data_collection_ins = insert(data_collection_table).values(
            shop_id=shop_id,
            date_collected=datetime_collected.date(),
            time_collected=datetime_collected.time(),
        )
        session.execute(data_collection_ins)

        # Commit the transaction
        session.commit()
