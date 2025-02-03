import mysql.connector
import pandas as pd
from sqlalchemy import create_engine,text

# sQLAlchemy motorunu oluşturuyoruz
engine = create_engine('mysql+mysqlconnector://root:westottomancompany1.E@localhost/emre')

# Veritabanı bağlantısını kontrol ediyoruz
connection = engine.connect()

# csV dosyalarının yolları
cencus_file = "G:/Data science/Accessing Databases using python/Final Assignment/csv_dosyalari/chicagocensusData.csv"
crime_file = "G:/Data science/Accessing Databases using python/Final Assignment/csv_dosyalari/chicagocrimeData.csv"
public_file = "G:/Data science/Accessing Databases using python/Final Assignment/csv_dosyalari/chicagopublicschools.csv"

# csV dosyalarını pandas DataFrame olarak yükleyelim
df_census = pd.read_csv(cencus_file)
df_crime = pd.read_csv(crime_file)
df_public = pd.read_csv(public_file)

# Veritabanına tablo eklemeye çalışıyoruz
try:
    df_census.to_sql(name='chicagocensus', con=engine, if_exists='replace', index=False, method='multi')
    print("chicagocensus tablosu başarıyla eklendi.")
except Exception as e:
    print("chicagocensus tablosu eklenirken hata oluştu:", e)

try:
    df_crime.to_sql(name="chicagocrime", con=engine, if_exists='replace', index=False, method='multi')
    print("chicagocrime tablosu başarıyla eklendi.")
except Exception as e:
    print("chicagocrime tablosu eklenirken hata oluştu:", e)

try:
    df_public.to_sql(name='chicagopublicschool', con=engine, if_exists='replace', index=False, method='multi')
    print("chicagopublicschool tablosu başarıyla eklendi.")
except Exception as e:
    print("chicagopublicschool tablosu eklenirken hata oluştu:", e)

with engine.connect() as connection:
    query = text("""
    SELECT COMMUNITY_AREA_NUMBER, COUNT(*) as crime_count
    from chicagocrime
    group by COMMUNITY_AREA_NUMBER
    order by crime_count desc 
    LIMIT 1
""")
    result = connection.execute(query)
    for row in result:
        print(row)

