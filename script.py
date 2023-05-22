import requests
import time
import pandas as pd
from sqlalchemy import create_engine, text
import matplotlib.pyplot as plt
import mysql.connector

# url of the csv file
csv_url = "https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1156715/ai-findings-2023_wk19.csv"

# function to download the csv file
def download_csv():
    response = requests.get(csv_url)
    if response.status_code == 200:
        with open("data.csv", "wb") as file:
            file.write(response.content)
        print("The csv file was successfully downloaded.")
    else:
        print("Failed to download the csv file.")

while True:
    download_csv()

    # normalise data
    value = "unknown"

    df = pd.read_csv("data.csv")
    df.fillna(value, inplace=True)
    df.to_csv("nonulldata.csv", index=False)

    # show empty cells are filled now
    df_ = pd.read_csv('nonulldata.csv')
    last_rows = df_.tail(10)

    print(last_rows)

    # remove duplicate data
    df1 = pd.read_csv("nonulldata.csv")
    duplicate_rows = df1[df1.duplicated()]

    print("\n\nDuplicate Rows : \n {}".format(duplicate_rows))

    remove_duplicates = df1.drop_duplicates(keep='last')
    print('\n\nData after duplicate removal :\n', remove_duplicates.head(30))

    df1.to_csv("transformeddata.csv", index=False)

    # create database
    engine = create_engine("mysql+mysqldb://root:EU.merg.10.mare@localhost/")
    connection = engine.connect()
    connection.execute(text("CREATE DATABASE IF NOT EXISTS BIRDFLUcases"))
    connection.close()

    print("Database was successfully created!")

    # insert csv data into the database
    csv_file = 'transformeddata.csv'

    df = pd.read_csv(csv_file)
    engine = create_engine("mysql://root:EU.merg.10.mare@localhost/BIRDFLUcases")
    # convert dataframe into sql and insert into the database
    df.to_sql(name="csvimport", con=engine, if_exists='append', index=False)
    engine.dispose()

    print("Done!")

    # connect to database
    db_connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="EU.merg.10.mare",
    database="BIRDFLUcases"
    )

    # query the database
    query = "SELECT * FROM BIRDFLUcases.csvimport"
    df = pd.read_sql_query(query, db_connection)
    db_connection.close()

    # group data by total for each county
    df_county = df.groupby('County')['Total'].sum().reset_index()
    df_county_sort = df_county.sort_values(by='Total', ascending=True)

    # group data by total for each district
    df_district = df.groupby('District')['Total'].sum().reset_index()
    df_district_sort = df_district.sort_values(by='Total', ascending=True)

    # group data by total for each week
    df_week = df.groupby('Week')['Total'].sum().reset_index()
    df_week_sort = df_week.sort_values(by='Total', ascending=True)

    # plot bar charts
    fig1 = plt.figure(figsize=(12,6))
    plt.bar(df_county_sort['County'], df_county_sort['Total'])
    plt.xlabel('County')
    plt.ylabel('Number of cases')
    plt.title('Total Cases by County')
    plt.xticks(rotation=90)
    plt.tight_layout()

    fig2 = plt.figure(figsize=(15,6))
    plt.bar(df_district_sort['District'], df_district_sort['Total'])
    plt.xlabel('District')
    plt.ylabel('Number of cases')
    plt.title('Total Cases by District')
    plt.xticks(rotation=90)
    plt.tight_layout()

    fig3 = plt.figure()
    plt.bar(df_week_sort['Week'], df_week_sort['Total'])
    plt.xlabel('Week')
    plt.ylabel('Number of cases')
    plt.title('Total Cases by Week')
    plt.xticks(rotation=45)
    plt.tight_layout()

    plt.show()

    # wait for 604800 seconds (a week) to download again
    time.sleep(604800)