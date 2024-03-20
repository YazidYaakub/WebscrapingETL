CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
   NEW.date_modified = now(); 
   RETURN NEW; 
END;
$$ LANGUAGE plpgsql;


CREATE TABLE states (
    state_code INT PRIMARY KEY,
    state_name VARCHAR(255) NOT NULL,
    date_modified TIMESTAMP WITH TIME ZONE DEFAULT (current_timestamp AT TIME ZONE 'Asia/Kuala_Lumpur'),
    CONSTRAINT unique_state_name UNIQUE (state_name)
);

CREATE TABLE districts (
    district_id SERIAL PRIMARY KEY,
    district_name VARCHAR(255) NOT NULL,
    date_modified TIMESTAMP WITH TIME ZONE DEFAULT (current_timestamp AT TIME ZONE 'Asia/Kuala_Lumpur'),
    CONSTRAINT unique_district_name UNIQUE (district_name)
);

CREATE TABLE coordinates (
    coordinate_id SERIAL PRIMARY KEY,
    latitude FLOAT,
    longitude FLOAT,
    date_modified TIMESTAMP WITH TIME ZONE DEFAULT (current_timestamp AT TIME ZONE 'Asia/Kuala_Lumpur')
);

CREATE TABLE shops (
    shop_id VARCHAR(255) PRIMARY KEY,
    shopname VARCHAR(255) NOT NULL,
    district_id INT,
    state_code INT,
    coordinate_id INT,
    googlemap_link TEXT,
    date_modified TIMESTAMP WITH TIME ZONE DEFAULT (current_timestamp AT TIME ZONE 'Asia/Kuala_Lumpur'),
    FOREIGN KEY (district_id) REFERENCES districts(district_id),
    FOREIGN KEY (state_code) REFERENCES states(state_code),
    FOREIGN KEY (coordinate_id) REFERENCES coordinates(coordinate_id)
);

CREATE TABLE tags (
    tag_id SERIAL PRIMARY KEY,
    tag_description VARCHAR(255) NOT NULL,
    date_modified TIMESTAMP WITH TIME ZONE DEFAULT (current_timestamp AT TIME ZONE 'Asia/Kuala_Lumpur'),
    CONSTRAINT unique_tag_description UNIQUE (tag_description)
);

CREATE TABLE shop_tags (
    shop_id VARCHAR(255),
    tag_id INT,
    PRIMARY KEY (shop_id, tag_id),
    date_modified TIMESTAMP WITH TIME ZONE DEFAULT (current_timestamp AT TIME ZONE 'Asia/Kuala_Lumpur'),
    FOREIGN KEY (shop_id) REFERENCES shops(shop_id),
    FOREIGN KEY (tag_id) REFERENCES tags(tag_id)
);

CREATE TABLE social_media_links (
    shop_id VARCHAR(255) PRIMARY KEY,
    instagram_link TEXT,
    facebook_link TEXT,
    twitter_link TEXT,
    tiktok_link TEXT,
    whatsapp_link TEXT,
    date_modified TIMESTAMP WITH TIME ZONE DEFAULT (current_timestamp AT TIME ZONE 'Asia/Kuala_Lumpur'),
    FOREIGN KEY (shop_id) REFERENCES shops(shop_id)
);

CREATE TABLE data_collection (
    shop_id VARCHAR(255) PRIMARY KEY,
    date_collected DATE,
    time_collected TIME,
    date_modified TIMESTAMP WITH TIME ZONE DEFAULT (current_timestamp AT TIME ZONE 'Asia/Kuala_Lumpur'),
    FOREIGN KEY (shop_id) REFERENCES shops(shop_id)
);

CREATE TRIGGER update_states_modtime BEFORE UPDATE ON states FOR EACH ROW EXECUTE FUNCTION update_modified_column();
CREATE TRIGGER update_districts_modtime BEFORE UPDATE ON districts FOR EACH ROW EXECUTE FUNCTION update_modified_column();
CREATE TRIGGER update_coordinates_modtime BEFORE UPDATE ON coordinates FOR EACH ROW EXECUTE FUNCTION update_modified_column();
CREATE TRIGGER update_shops_modtime BEFORE UPDATE ON shops FOR EACH ROW EXECUTE FUNCTION update_modified_column();
CREATE TRIGGER update_tags_modtime BEFORE UPDATE ON tags FOR EACH ROW EXECUTE FUNCTION update_modified_column();
CREATE TRIGGER update_shop_tags_modtime BEFORE UPDATE ON shop_tags FOR EACH ROW EXECUTE FUNCTION update_modified_column();
CREATE TRIGGER update_social_media_links_modtime BEFORE UPDATE ON social_media_links FOR EACH ROW EXECUTE FUNCTION update_modified_column();
CREATE TRIGGER update_data_collection_modtime BEFORE UPDATE ON data_collection FOR EACH ROW EXECUTE FUNCTION update_modified_column();





