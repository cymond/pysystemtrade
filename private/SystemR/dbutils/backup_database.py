import pandas as pd
from sqlalchemy import create_engine
backup_dir = "~/backup/"
engine = create_engine('mysql+pymysql://root:admin@0.0.0.0/pkdemo')
control_tables = ["roll_schedule", "instrumentconfig","costs_analysis", "marketdata"]
dict = {}
for table in control_tables:
    var_df = table + "df"
    var_df = pd.read_sql_table(table_name=table, con=engine)
    dict[table] = var_df
    print("retrieving " + table + "...")
    #print(var_df)
    filename = backup_dir + table + ".csv"
    var_df.to_csv(filename, index=False)
for row in dict["marketdata"].itertuples():
    print("===============================================")
    files = []
    for line in (dict["roll_schedule"][(dict["roll_schedule"]['CARVER'] == row.CARVER)]).itertuples():
        files.append(line.CARVER.lower() + line.CARRY_CONTRACT)
        files.append(line.CARVER.lower() + line.PRICE_CONTRACT)
    for x in (list(set(files))):
        print(x)
        var_df = pd.read_sql_table(table_name=x, con=engine)
        filename = backup_dir + x + ".csv"
        print("...writing file: ", filename)
        var_df.to_csv(filename, index=False)




#for row in dict["roll_schedule"].itertuples():
#    print(row)

# Backup the raw data for each market
# For each market in marketdata backup a;; corresponding raw data contracts in roll-schedule