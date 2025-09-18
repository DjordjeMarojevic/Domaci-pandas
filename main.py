import mysql.connector
import pandas as pd
import numpy as np

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="root",
  database="domaci"
)


df = pd.read_sql("SELECT * FROM studenti", con=mydb)
print(df.head(10))

print(df.info())
print(df.describe())

rows, cols = df.shape

print(f"Dataset ima {rows} redova i {cols} kolona.")

print("--------------------------------------------")

empty_counts = (df == 'NaN').sum()
print(empty_counts)
print(empty_counts[empty_counts > 0])

grad_map = {
    "pg": "Podgorica",
    "Podgorica ": "Podgorica",
    "pg ": "Podgorica",
    "novi sad": "Novi Sad",
    "NIŠ": "Niš"
 
}

cursor = mydb.cursor()
table_name = "studenti"


cursor.execute(f"ALTER TABLE {table_name} MODIFY COLUMN Godine VARCHAR(255);")
mydb.commit()

df.replace(["", "NULL", "NaN"], np.nan, inplace=True)


df["Godine"] = pd.to_numeric(df["Godine"], errors="coerce")
df['Godine'] = df['Godine'].abs()
mean_years = df["Godine"].mean()
my = mean_years.astype(str)
df.loc[df["Godine"].isna(), "Godine"] = my

df["Prosek"] = pd.to_numeric(df["Prosek"], errors="coerce")
df['Prosek'] = df['Prosek'].clip(upper=10)
median_prosek = df["Prosek"].median()
mp = median_prosek.astype(str)
df.loc[df["Prosek"].isna(), "Prosek"] = mp

df["Grad"].replace([""," " ,"NULL", "NaN"], np.nan, inplace=True)
df.loc[df["Grad"].isna(), "Grad"] = "nepoznat"
df["Grad"] = df["Grad"].replace(grad_map)


def standardize_phone(phone):
    if pd.isna(phone):
        return None 
   
    if not phone.startswith('+'):
        phone = '+381' + phone[:]    
    return phone

df['Telefon'] = df['Telefon'].apply(standardize_phone)


for index, row in df.iterrows():
    sql = f"""
        UPDATE {table_name}
        SET Godine = %s,
            Prosek = %s,
            Grad = %s,
            Telefon = %s
        WHERE ID = %s
    """
    values = (
        row["Godine"],  
        row["Prosek"], 
        row["Grad"],
        row['Telefon'],
        row["ID"] 
    )
    cursor.execute(sql, values)

delete_sql = """
DELETE FROM studenti
WHERE Email NOT LIKE '%@%' OR Email NOT LIKE '%.%';
"""
cursor.execute(delete_sql)

mydb.commit()


def safe_to_datetime(value):
    if pd.isna(value) or str(value).strip() == "":
        return value 
    return pd.to_datetime(value, errors='coerce')

df['Datum_upisa'] = df['Datum_upisa'].apply(safe_to_datetime)
df['Datum_diplomiranja'] = df['Datum_diplomiranja'].apply(safe_to_datetime)

df['Trajanje_studija'] = ((df['Datum_diplomiranja'] - df['Datum_upisa']).dt.days / 365.25).round(1)
df['Trajanje_studija'] = df['Trajanje_studija'].astype(str)
mydb.commit()

for index, row in df.iterrows():
    sql = f"""
        UPDATE {table_name}
        SET Trajanje_studija = %s
        WHERE ID = %s
    """
    values = (row['Trajanje_studija'], row['ID'])
    cursor.execute(sql, values)

mydb.commit()
print("Baza je ažurirana!")


df['Trajanje_studija'] = pd.to_numeric(df['Trajanje_studija'], errors='coerce')

max_idx = df['Trajanje_studija'].idxmax()
student_max = df.loc[max_idx]

min_idx = df['Trajanje_studija'].idxmin()
student_min = df.loc[min_idx]

print("Student sa najdužim trajanjem studija:")
print(student_max)

print("\nStudent sa najkraćim trajanjem studija:")
print(student_min)


