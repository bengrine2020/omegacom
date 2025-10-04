#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sqlalchemy
import pymysql
import pandas
import openpyxl
from dotenv import load_dotenv

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

from datetime import datetime



print("✅ All libraries are installed correctly")


# # -----------------------------
# # Database configuration
# # -----------------------------

# In[2]:


load_dotenv()

USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD")
HOST = os.getenv("DB_HOST", "localhost")
PORT = int(os.getenv("DB_PORT", 3306))
NAME=os.getenv("DB_NAME")



TABLE_NAME = "prestation"  # your table

engine = create_engine(
    f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{NAME}"
)


# # -----------------------------
# # Read Excel file and preprocess
# # -----------------------------

# In[3]:





# excel_file = "data/pronatech_data.xlsx" 
# excel_file = "data/Pronatech/Export_vorder_interne_9_2025-4.xlsx" 
excel_file="data/Pronatech/Export_vorder_interne_10_2025-3_1655.xlsx"
df = pd.read_excel(excel_file)

# Rename columns to match DB
df = df.rename(columns={
    "Koopnummer": "Koopnummer",
    "Roepnummer": "Roepnummer",
    "Local_Net": "Local_Net",
    "Werkbevelnummer": "Werkbevelnummer",
    "Itemnummer": "Itemnummer",
    "Aantal": "Aantal",
    "REF Nbr": "REF_Nbr",
    "Datum": "Datum",
    "Pernr": "Pernr",
    "Opmerkingen": "Opmerkingen",
    "Control": "Control",
    "Corrections BGC": "Corrections_BGC",
    "Corrections Contractor": "Corrections_Contractor",
    "Prix": "Prix",
    "Points": "Points",
    "Société": "Societe"
})

# Replace NaN with None for MySQL
df = df.where(pd.notnull(df), None)

# Convert column types
df['Datum'] = pd.to_datetime(df['Datum'], dayfirst=True)
str_cols = ['Roepnummer','Itemnummer','REF_Nbr','Control','Corrections_BGC','Corrections_Contractor']
for col in str_cols:
    df[col] = df[col].fillna('').astype(str)

float_cols = ['Prix','Points']
for col in float_cols:
    df[col] = df[col].astype(float)


# In[4]:


month_str = df['Datum'].dt.strftime("%Y-%m").iloc[0]
month_str


# # -----------------------------
# # Sync logic
# # -----------------------------
# # Extract the month of the Excel data (assume all rows are for the same month)

# In[5]:


# Get the month of the first entry in the DataFrame
month_str = df['Datum'].dt.strftime("%Y-%m").iloc[0]

# Fetch existing rows for the same month from the database
with engine.connect() as conn:
    # Fetch existing rows for the same month
    existing_df = pd.read_sql(
        text(f"SELECT * FROM {TABLE_NAME} WHERE DATE_FORMAT(Datum, '%Y-%m') = :month"),
        conn,
        params={"month": month_str}
    )

# Define key columns for comparison
key_columns = ['Koopnummer','Roepnummer','Itemnummer']


# In[6]:




# Determine rows to insert
merged_insert = df.merge(existing_df[key_columns], on=key_columns, how='left', indicator=True)
to_insert = merged_insert[merged_insert['_merge']=='left_only'].drop(columns=['_merge'])


# In[7]:




# Insert new rows
with engine.begin() as conn:
    for _, row in to_insert.iterrows():
        sql = text(f"""
            INSERT INTO {TABLE_NAME} ({', '.join(df.columns)})
            VALUES ({', '.join(':' + c for c in df.columns)})
        """)
        conn.execute(sql, row.to_dict())


# Determine rows to soft-delete (update deleted_at)
merged_delete = existing_df.merge(df[key_columns], on=key_columns, how='left', indicator=True)
to_delete = merged_delete[merged_delete['_merge']=='left_only']


with engine.begin() as conn:
    for _, row in to_delete.iterrows():
        sql = text(f"""
            UPDATE {TABLE_NAME}
            SET deleted_at = :deleted_at
            WHERE {" AND ".join(f"{c} = :{c}" for c in key_columns)}
        """)
        params = {c: row[c] for c in key_columns}
        params['deleted_at'] = datetime.now()
        conn.execute(sql, **params)

print("✅ Excel data synced with database (inserted new rows, soft-deleted missing rows).")

