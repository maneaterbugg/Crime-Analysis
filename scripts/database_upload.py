import pandas as pd
from sqlalchemy import create_engine, text
from config.config import DB_USER, DB_PASSWORD, DB_HOST, DB_NAME

# SQLAlchemy motorunu oluştur
engine = create_engine(f'mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}')

# CSV dosyalarının yolları
census_file = "../csv_files/chicagocensusData.csv"
crime_file = "../csv_files/chicagocrimeData.csv"
public_file = "../csv_files/chicagopublicschools.csv"

# CSV dosyalarını DataFrame olarak yükle
df_census = pd.read_csv(census_file)
df_crime = pd.read_csv(crime_file)
df_public = pd.read_csv(public_file)

# Veritabanına tabloları ekleme
def upload_to_db(df, table_name):
    try:
        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False, method='multi')
        print(f"{table_name} tablosu başarıyla eklendi.")
    except Exception as e:
        print(f"{table_name} tablosu eklenirken hata oluştu: {e}")

upload_to_db(df_census, 'chicagocensus')
upload_to_db(df_crime, 'chicagocrime')
upload_to_db(df_public, 'chicagopublicschool')

# SQL sorgusu ile en çok suç işlenen bölgeyi bul
with engine.connect() as connection:
    query = text("""
        SELECT COMMUNITY_AREA_NUMBER, COUNT(*) as crime_count
        FROM chicagocrime
        GROUP BY COMMUNITY_AREA_NUMBER
        ORDER BY crime_count DESC 
        LIMIT 1
    """)
    result = connection.execute(query)
    for row in result:
        print(f"En fazla suç işlenen bölge: {row}")
