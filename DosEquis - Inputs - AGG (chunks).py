import os
import glob
import pandas as pd
from pathlib import Path
import datetime  
import numpy as np  
import re

Path = r'C:\Users\makambou2001\Desktop\Dos Equis\Oh here we go again (2022)\New'
#Input_level = "Total"    #Total or Channel 
#Geog_Lvl = "Store"       #Store or Region
#channel_dict= {"C STORES": "CONVENIENCE", "OFF TRADE": "TRADITIONAL"}


start = datetime.datetime.now()

Semanas = pd.read_csv(os.path.join(Path, 'MapNielsenWeeks.csv'), sep = ',')
Semanas["date"] = pd.to_datetime(Semanas["date"])
PPGs = pd.read_csv(os.path.join(Path, 'PPGs.csv'), sep = ',').rename(columns={"sku": "sku_code"})
Nielsen_area = pd.read_csv(os.path.join(Path, "MapAreasNielsen.csv"), sep = ',')

Sales_list_2019 = []
Sales_list_2020 = []
Sales_list_2021 = []

for chunks_2019 in pd.read_csv(os.path.join(Path, 'Profitability_XX_2019.csv'), sep =',', encoding = 'latin-1', chunksize=100000):
    chunks_2019.volume.replace(np.nan, 0, inplace=True)
    chunks_2019.state.replace(np.nan, "XX", inplace=True)
    chunks_2019.channel.replace(np.nan, "XX", inplace=True)
    chunks_2019["date"] = pd.to_datetime(chunks_2019["date"])
    chunk_new = chunks_2019.loc[(chunks_2019["volume"] >= 0) & (chunks_2019["line_extension"] != "Dos Equis Radler") & (~chunks_2019.channel.isin(["EVENTS", "ON TRADE", "XX"])),:].copy()
    chunk_merge = pd.merge(pd.merge(chunk_new, PPGs, how = "left", on = "sku_code"), Semanas, how = "left", on = "date")
    chunk_agg = chunk_merge.groupby(["NielsenDate", "client_code", "channel", "state", "PPG", "brand_family", "line_extension"], as_index = False).agg({"volume":"sum", "income":"sum", "promo_spend":"sum"})
    Sales_list_2019.append(chunk_agg)

Sales_2019 = pd.concat(Sales_list_2019)
print("2019...Post chunk tenemos " + str(len(Sales_2019)) + " lineas")
print("2019...Post chunk tenemos " + str(Sales_2019.volume.sum()))
print('Vamos ', datetime.datetime.now() - start)

Sales_agg_2019 = Sales_2019.groupby(["NielsenDate", "client_code", "channel", "state", "PPG", "brand_family", "line_extension"], as_index = False).agg({"volume":"sum", "income":"sum", "promo_spend":"sum"})
print("2019...Post agg tenemos " + str(len(Sales_agg_2019)) + " lineas")
print("2019...Post agg tenemos " + str(Sales_agg_2019.volume.sum()))
print('Despues de agregar... ', datetime.datetime.now() - start)

for chunks_2020 in pd.read_csv(os.path.join(Path, 'Profitability_XX_2020.csv'), sep =',', encoding = 'latin-1', chunksize=100000):
    chunks_2020.volume.replace(np.nan, 0, inplace=True)
    chunks_2020.state.replace(np.nan, "XX", inplace=True)
    chunks_2020.channel.replace(np.nan, "XX", inplace=True)
    chunks_2020["date"] = pd.to_datetime(chunks_2020["date"])
    chunks_2020['month'] = chunks_2020['date'].dt.month
    chunk_new = chunks_2020.loc[(chunks_2020["month"] != 5) & (chunks_2020["volume"] >= 0) & (chunks_2020["line_extension"] != "Dos Equis Radler") & (~chunks_2020.channel.isin(["EVENTS", "ON TRADE", "XX"])),:].copy()
    chunk_merge = pd.merge(pd.merge(chunk_new, PPGs, how = "left", on = "sku_code"), Semanas, how = "left", on = "date")
    chunk_agg = chunk_merge.groupby(["NielsenDate", "client_code", "channel", "state", "PPG", "brand_family", "line_extension"], as_index = False).agg({"volume":"sum", "income":"sum", "promo_spend":"sum"})
    Sales_list_2020.append(chunk_agg)

Sales_2020 = pd.concat(Sales_list_2020)
print("2020...Post chunk tenemos " + str(len(Sales_2020)) + " lineas")
print("2020...Post chunk tenemos " + str(Sales_2020.volume.sum()))
print('Vamos ', datetime.datetime.now() - start)

Sales_agg_2020 = Sales_2020.groupby(["NielsenDate", "client_code", "channel", "state", "PPG", "brand_family", "line_extension"], as_index = False).agg({"volume":"sum", "income":"sum", "promo_spend":"sum"})
print("2020...Post agg tenemos " + str(len(Sales_agg_2020)) + " lineas")
print("2020...Post agg tenemos " + str(Sales_agg_2020.volume.sum()))
print('Despues de agregar... ', datetime.datetime.now() - start)

for chunks_2021 in pd.read_csv(os.path.join(Path, 'Profitability_db_2021.csv'), sep =',', encoding = 'latin-1', chunksize=100000):
    chunks_2021.volume.replace(np.nan, 0, inplace=True)
    chunks_2021.state.replace(np.nan, "XX", inplace=True)
    chunks_2021.channel.replace(np.nan, "XX", inplace=True)
    chunks_2021["date"] = pd.to_datetime(chunks_2021["date"])
    chunk_new = chunks_2021.loc[(chunks_2021["volume"] >= 0) & (chunks_2021["line_extension"] != "Dos Equis Radler") & (~chunks_2021.channel.isin(["EVENTS", "ON TRADE ", "ON TRADE", "XX"])),:].copy()
    chunk_new.channel.replace("C STORES ", "C STORES", inplace = True)
    chunk_merge = pd.merge(pd.merge(chunk_new, PPGs, how = "left", on = "sku_code"), Semanas, how = "left", on = "date")
    chunk_agg = chunk_merge.groupby(["NielsenDate", "client_code", "channel", "state", "PPG", "brand_family", "line_extension"], as_index = False).agg({"volume":"sum", "income":"sum", "promo_spend":"sum"})
    Sales_list_2021.append(chunk_agg)

Sales_2021 = pd.concat(Sales_list_2021)
print("2021...Post chunk tenemos " + str(len(Sales_2020)) + " lineas")
print("2021...Post chunk tenemos " + str(Sales_2020.volume.sum()))
print('Vamos ', datetime.datetime.now() - start)

Sales_agg_2021 = Sales_2021.groupby(["NielsenDate", "client_code", "channel", "state", "PPG", "brand_family", "line_extension"], as_index = False).agg({"volume":"sum", "income":"sum", "promo_spend":"sum"})
print("2021...Post agg tenemos " + str(len(Sales_agg_2021)) + " lineas")
print("2021...Post agg tenemos " + str(Sales_agg_2021.volume.sum()))
print('Despues de agregar... ', datetime.datetime.now() - start)



Total_sales = pd.concat([Sales_agg_2019, Sales_agg_2020, Sales_agg_2021])
Total_sales = Total_sales.groupby(["NielsenDate", "client_code", "channel", "state", "PPG", "brand_family", "line_extension"], as_index = False).agg({"volume":"sum", "income":"sum", "promo_spend":"sum"})
Total_sales.to_csv(os.path.join(Path, "Total_sales.csv"), index = None)
print('Listo, terminamos ', datetime.datetime.now() - start)

print(Total_sales.channel.drop_duplicates())

New_sales = Total_sales.groupby(["PPG", "channel"], as_index = False).agg({"volume":"sum", "income":"sum", "promo_spend":"sum"})
New_sales.to_csv(os.path.join(Path, "sal.csv"), index = None)
"""
Semanas.info()

Sales_Sem = pd.merge(Sales, Semanas, how = "left", on = "date")

Sales_Sem.info()
Sales_Sem.head(10)






print("El numero de lineas original es " + str(len(Sales)))
print("El volumen original es " + str(Sales.volume.sum()))

Sales = Sales.loc[(Sales["volume"] > 0) & (Sales["line_extension"] != "Dos Equis Radler") & (~Sales.channel_type.isin(["EVENTS", "ON TRADE", "0"])),["Fecha Nielsen", "line_extension", "PPG", "channel_type", "Area Nielsen", "client_code", "volume"]]
#Sales["Fecha Nielsen"] = pd.to_datetime(Sales["Fecha Nielsen"], format = "%m/%d/%Y")

Sales_merge = pd.merge(Sales, Estados, how = 'left', on = 'client_code')
print("Post filtro tenemos " + str(len(Sales_merge)) + " lineas")
print("Post filtro tenemos " + str(Sales_merge.volume.sum()))

if Geog_Lvl == "Store":
    Sales_merge["store"] = Sales_merge.client_code.rank(method = "dense")
    Sales_merge["store"] = Sales_merge["store"].astype(int)
    Sales_merge["store_new"] = str("GEOG") + Sales_merge["store"].astype(str)
else:
    Sales_merge["State_New"] = Sales_merge["state"] + Sales_merge["channel_type"] + Sales_merge["Area Nielsen"].astype(str)
    Sales_merge["store"] = Sales_merge.State_New.rank(method = "dense")
    Sales_merge["store"] = Sales_merge["store"].astype(int)
    Sales_merge["store_new"] = str("GEOG") + Sales_merge["store"].astype(str)

Sales_merge["channel_type"] = Sales_merge["channel_type"].replace(channel_dict)
Sales_merge["Geogkey"] = Sales_merge["channel_type"].str[0:4] + str("@") + Sales_merge["Area Nielsen"].astype(str)

# Sales_merge["Product"] = Sales_merge["line_extension"] + str(" ") + Sales_merge["PPG"]
Sales_merge["Product"] = Sales_merge["PPG"]
Sales_merge["Prod"] = Sales_merge.Product.rank(method = "dense").astype(int)
Sales_merge["Prodkey"] = str("PROD") + Sales_merge["Prod"].astype(str)


Channels = list(Sales_merge.channel_type.drop_duplicates())
print(Channels)

if Input_level == "Channel":
    for x in Channels:
        Sales_channel = Sales_merge.loc[Sales_merge["channel_type"] == x, ["Prodkey", "Product", "store_new", "Fecha Nielsen","Geogkey", "volume"]].copy().rename(columns={'Fecha Nielsen':'Week', 'volume': 'Volume'})
        Sales_channel = Sales_channel.groupby(["Prodkey", "Product", "Week", "store_new", "Geogkey"], as_index = False).agg({"Volume":"sum"})
        Sales_channel["Price"] = 1
        Sales_channel["Value"] = Sales_channel["Volume"] * Sales_channel["Price"]
        Inputdata = Sales_channel.loc[:,["Prodkey", "store_new", "Week", "Volume", "Price", "Value"]].copy().rename(columns={"store_new":"Geogkey"})
        Inputdata["Week"] = pd.to_datetime(Inputdata["Week"])
        print(Inputdata.duplicated(subset = ["Prodkey", "Geogkey", "Week"]).sum())
        Geogmaster = Sales_channel.loc[:, ["store_new", "Geogkey"]].copy().drop_duplicates().rename(columns = {"store_new":"Store", "Geogkey":"MKT"})
        Prodmast = Sales_channel.loc[:, ["Prodkey", "Product"]].copy().drop_duplicates().rename(columns = {"Prodkey":"PROD", "Product":"prodname"})

        if not os.path.exists(os.path.join(Path, x)):
            os.mkdir(os.path.join(Path, x))

        Inputdata.to_csv(os.path.join(Path, x, "Inputdata.csv"), index = None, date_format = "%m/%d/%Y")
        Geogmaster.to_csv(os.path.join(Path, x, "BaseGeogmast.csv"), index = None)
        Prodmast.to_csv(os.path.join(Path, x, "BaseProdmast.csv"), index = None)
else:
    Sales_channel = Sales_merge.loc[:, ["Prodkey", "Product", "store_new", "Fecha Nielsen","Geogkey", "volume"]].copy().rename(columns={'Fecha Nielsen':'Week', 'volume': 'Volume'})
    Sales_channel = Sales_channel.groupby(["Prodkey", "Product", "Week", "store_new", "Geogkey"], as_index = False).agg({"Volume":"sum"})
    Sales_channel["Price"] = 1
    Sales_channel["Value"] = Sales_channel["Volume"] * Sales_channel["Price"]
    Inputdata = Sales_channel.loc[:,["Prodkey", "store_new", "Week", "Volume", "Price", "Value"]].copy().rename(columns={"store_new":"Geogkey"})
    Inputdata["Week"] = pd.to_datetime(Inputdata["Week"])
    Geogmaster = Sales_channel.loc[:, ["store_new", "Geogkey"]].copy().drop_duplicates().rename(columns = {"store_new":"Store", "Geogkey":"MKT"})
    Prodmast = Sales_channel.loc[:, ["Prodkey", "Product"]].copy().drop_duplicates().rename(columns = {"Prodkey":"PROD", "Product":"prodname"})

    if not os.path.exists(os.path.join(Path, "Total")):
        os.mkdir(os.path.join(Path, "Total"))

    Inputdata.to_csv(os.path.join(Path, "Total", "Inputdata.csv"), index = None, date_format = "%m/%d/%Y")
    Geogmaster.to_csv(os.path.join(Path, "Total", "BaseGeogmast.csv"), index = None)
    Prodmast.to_csv(os.path.join(Path, "Total", "BaseProdmast.csv"), index = None)



#Transposed = Sales_merge.pivot_table(index = ["Prodkey", "Geogkey", "store_new"], columns = "Fecha Nielsen", values = "volume", aggfunc = np.sum, fill_value = 0).reset_index()
#Transposed.to_csv(os.path.join(Path, 'Transposed.csv'), index = None)

#Sales_merge.to_csv(os.path.join(Path, "Sales_merge.csv"), index = None)
print('Listo, terminamos ', datetime.datetime.now() - start)
"""