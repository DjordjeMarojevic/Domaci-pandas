import mysql.connector
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

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
    "NIŠ": "Niš",
    "Nis": "Niš"
 
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


df['Godine'] = pd.to_numeric(df['Godine'], errors='coerce')

plt.hist(df['Godine'].dropna(), bins=20, edgecolor='black')

plt.title("Raspodjela godina studenata")
plt.xlabel("Godine")
plt.ylabel("Broj studenata")
plt.grid(axis='y', linestyle='--', alpha=0.7)

plt.savefig("histogram_godine.png")

df['Prosek'] = pd.to_numeric(df['Prosek'], errors='coerce')

plt.figure(figsize=(6, 8))
plt.boxplot(df['Prosek'].dropna(), vert=True, patch_artist=True,
            boxprops=dict(facecolor="lightblue", color="blue"),
            medianprops=dict(color="red"),
            whiskerprops=dict(color="black"),
            capprops=dict(color="black"),
            flierprops=dict(markerfacecolor="orange", marker="o", markersize=8))

plt.title("Boxplot raspodjele prosjeka studenata")
plt.ylabel("Prosjek")
plt.grid(axis="y", linestyle="--", alpha=0.7)

plt.savefig("boxplot_prosek.png")

avg_by_city = df.groupby('Grad')['Prosek'].mean().sort_values(ascending=False)

plt.figure(figsize=(10, 6))
avg_by_city.plot(kind='bar', color='skyblue', edgecolor='black')

plt.title("Prosjecan prosjek po gradu")
plt.xlabel("Grad")
plt.ylabel("Prosjecan prosjek")
plt.xticks(rotation=45, ha='right')
plt.grid(axis='y', linestyle='--', alpha=0.7)

plt.tight_layout()
plt.savefig("bar_chart_prosek_po_gradu.png")

df['ESPB'] = pd.to_numeric(df['ESPB'], errors='coerce')

plt.figure(figsize=(12, 6))
df.boxplot(column="ESPB", by="Grad", grid=False,
           boxprops=dict(color="blue"),
           medianprops=dict(color="red"),
           whiskerprops=dict(color="black"),
           capprops=dict(color="black"),
           flierprops=dict(markerfacecolor="orange", marker="o", markersize=6))

plt.title("Raspodjela ESPB po gradovima")
plt.suptitle("") 
plt.xlabel("Grad")
plt.ylabel("ESPB")
plt.xticks(rotation=45, ha="right")

plt.tight_layout()
plt.savefig("boxplot_espb_po_gradovima.png")

plt.figure(figsize=(8, 6))
plt.scatter(df['Godine'], df['Prosek'], alpha=0.7, color="royalblue", edgecolors="k")

plt.title("Godine vs. Prosjek")
plt.xlabel("Godine")
plt.ylabel("Prosjek")
plt.grid(linestyle="--", alpha=0.6)

plt.savefig("scatter_godine_vs_prosek.png")

df['Datum_upisa'] = pd.to_datetime(df['Datum_upisa'], errors='coerce')

df['Godina_upisa'] = df['Datum_upisa'].dt.year
avg_by_year = df.groupby('Godina_upisa')['Prosek'].mean().dropna()
plt.figure(figsize=(10, 6))
plt.plot(avg_by_year.index, avg_by_year.values, marker='o', linestyle='-', color='blue')

plt.title("Prosjecan prosjek po godini upisa")
plt.xlabel("Godina upisa")
plt.ylabel("Prosječan prosjek")
plt.grid(linestyle="--", alpha=0.7)

plt.savefig("linechart_prosek_po_godini_upisa.png")


print("-----------------------------------------------------------------")

df['Datum_upisa'] = pd.to_datetime(df['Datum_upisa'], errors='coerce')
df['Godina_upisa'] = df['Datum_upisa'].dt.year

pivot = pd.pivot_table(
    df,
    values='Prosek',
    index='Grad',
    columns='Godina_upisa',
    aggfunc='mean'
)

print("Prosjecan prosjek po gradu i godini upisa")
print(pivot)


print()
print("-------------------------------------")
top5 = df.sort_values(by="Prosek", ascending=False).head(5)

print("Top 5 studenata sa najvišim prosjekom:")
print(top5[['ID', 'Ime', 'Prosek']])


avg_espb_by_city = df.groupby('Grad')['ESPB'].mean().sort_values(ascending=False)

print("Prosjecan broj ESPB bodova po gradu:")
print(avg_espb_by_city)