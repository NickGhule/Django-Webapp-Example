#!/usr/bin/env python3
# %%


import pymongo

# Connect to the MongoDB, change the connection string per your MongoDB environment
client = pymongo.MongoClient(port=27017)
db = client.CRIS  # use CRIS database



# %%
# sample document
"""
{
    _id: ObjectId("63518894e3bb0cccd55c4e16"),
    LOCATION_ID: 18502,
    TICKET_NUMBER: '94QVDC8000',
    STOCK_NUMBER_C: 'UFI',
    STOCK_NUMBER_N: 1019876,
    SESSION_ID: 15603,
    STATION_FROM: 'KOP',
    STATION_UPTO: 'PUNE',
    CLASS_CODE: 'II',
    NUMBER_OF_CHILD: 0,
    NUMBER_OF_ADULT: 2,
    MODE_OF_PAYMENT_ID: 1,
    BASE_FARE: 228,
    ROUTE_ID: 2010150952,
    TXN_TYPE_ID: 1,
    TICKET_TYPE_ID: 1,
    TRAIN_TYPE_ID: 1,
    SUPERFAST_CHARGE: 0,
    AD_STATION_AMOUNT: 0,
    AD_ROUTE_AMOUNT: 0,
    JOURNEY_DATE: 'Oct  1 2022 12:00AM',
    TXN_TIME: 'Oct  1 2022  4:52:32:116AM',
    SECRET_NUMBER: 18148,
    SAFETY_CHARGE: 0,
    BUS_CHARGE: 0,
    CASH_RECEIVED: 230,
    DEBITABLE_AMOUNT: 0,
    ORIGINAL_TXN_TYPE_ID: 1,
    LOCAL_FOREIGN_ID: 1,
    CONCESSION_CODE: '',
    CONCESSION_FORM_NO: '',
    DEBITABLE_TO: '',
    NUMBER_OF_DEPEND: 0,
    VALID_UPTO: '',
    TRAIN_NUMBER: '',
    BANK_CODE: '',
    CREDIT_CARD_NUMBER: '',
    CHILD_BASE_FARE: 0,
    DISTANCE: 327,
    CONCESSION_FARE: 0,
    CONCESSION_PERCENTAGE: 0,
    TKT_SESSION_ID: 1,
    TERMINAL_CODE: 'KOP02C',
    MUTP_AMOUNT: 0,
    CIDCO_AMOUNT: 0,
    MMTS_AMOUNT: 0,
    FUEL_CHARGE: 9.19,
    RESVN_CHARGE: 0,
    SERVICE_TAX: 0,
    CATERING_CHARGE: 0,
    ADHOC_CHARGES: 0,
    ADHOC: '',
    CESS_AMOUNT: 0,
    ADDLT_CHARGE: 0,
    SUBURBAN_FLAG: 0,
    TICKET_TYPE_CODE: 'J',
    TRAIN_TYPE_CODE: 'E',
    OPERATOR_CODE: 'RUPALI',
    LOGIN_TIME: 'Oct  1 2022 12:10:13:860AM',
    WINDOW_NUMBER: 2,
    SHIFT_NUMBER: 1,
    LOCATION_CODE: 'KOP',
    DIVISION_CODE: 'PUNE',
    IGST: 0,
    CGST: 0,
    SGST: 0,
    UTGST: 0,
    CESS: 0,
    TOTAL_GST: 0
  }
  """

# %%

# fetch total passengers, total tickets, total earnings by LOCATION_CODE, CLASS_CODE, window_number, shift_number, operator_code
pipeline = [{"$group": \
                {"_id":{ \
                    "LOCATION_CODE":"$LOCATION_CODE", \
                        "CLASS_CODE":"$CLASS_CODE", \
                            "Window": "$WINDOW_NUMBER", \
                                "Shift": "$SHIFT_NUMBER", \
                                    "Operator" : "$OPERATOR_CODE",  \
                                        "TERMINAL_CODE" : "$TERMINAL_CODE"\
                                            }, \
                                                "PASSENGERS":{"$sum":{"$add":["$NUMBER_OF_ADULT","$NUMBER_OF_CHILD", "$NUMBER_OF_DEPEND"]}},\
                                                    "total tickets":{"$sum":1}, \
                                                        "Total Earning":{"$sum":{"$add":["$CASH_RECEIVED","$DEBITABLE_AMOUNT"]}} \
                                                            }}]


result = db.BJ1.aggregate(pipeline)                




    

# %%
data = []
for i in result:
    row =  i.pop("_id")
    row.update(i)
    data.append(row)

import pandas as pd
df = pd.DataFrame(data)

#sort by LOCATION_CODE, CLASS_CODE, window_number, shift_number, operator_code
# df.sort_values(by=['LOCATION_CODE', 'CLASS_CODE', 'Window', 'Shift', 'Operator', 'TERMINAL_CODE'], inplace=True)


# %%
df.sort_values(by=['LOCATION_CODE', 'CLASS_CODE', 'Window', 'Shift', 'Operator'], inplace=True)

# %%
df

# %%
# print the dataframe in a table format
from tabulate import tabulate
print(tabulate(df, headers='keys', tablefmt='psql'))



