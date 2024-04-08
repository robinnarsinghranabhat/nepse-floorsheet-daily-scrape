import pandas as pd
from datetime import date, timedelta
import os
from nepse import Nepse
import time
import random

def save_floorsheet(nepse, symbol, date, save_dir, cache):
    file_name = f'./{save_dir}/{symbol}-{date}.csv'
    
    if cache and os.path.exists(file_name):
        print("Caching ! ")
        return
    try:
        time.sleep(random.uniform(1,5)) # Deliberate Throttling
        data = nepse.getFloorSheetOf(symbol=symbol, business_date=date)
        if not data:
            print(f"No Data returned for {symbol}-{date}")

            return "Empty"
        data = pd.DataFrame(data)
        print(file_name)
        print(len(data))
        data.to_csv(file_name, index=False)
    except Exception as e:
        print("GOT ERROR : ", str(e))
        return {"ERROR" : e, 'symbol': symbol, 'date': date}


def save_floorsheet_day(symbols, date, save_dir, cache=True):

    if os.path.exists( f'./{save_dir}/{date}.gz' ):
        return
    
    nepse = Nepse()
    nepse.headers['Connection'] = 'close'
    nepse.setTLSVerification(False) # This is temporary, until nepse sorts its ssl certificate problem


    print("Processing : ", date)

    erred = []
    for symbol in symbols:
        print(f"Symbol : {symbol} - DATE : {date}")
        out = save_floorsheet(nepse, symbol, date, save_dir,cache)
        if out:
            erred.append(out) 

    # merge_csv_files(save_dir)
    return erred

def merge_csv_files(folder_path):
    # Check if the folder exists
    if not os.path.exists(folder_path):
        print(f"Folder '{folder_path}' not found.")
        return
    
    # Initialize an empty DataFrame to store the merged data
    merged_df = pd.DataFrame()
    save_name = None
    # Iterate through each file in the folder
    for filename in os.listdir(folder_path):
        # Check if the file is a CSV file
        if filename.endswith('.csv'):
            save_name = filename
            # Read the CSV file into a DataFrame
            file_path = os.path.join(folder_path, filename)
            df = pd.read_csv(file_path)
            # Append the data to the merged DataFrame
            merged_df = merged_df.append(df, ignore_index=True)
            # Remove the individual CSV file
            os.remove(file_path)
    if not save_name:
        print("Data not available")
        return
    # Get the date from the first file name
    date = save_name.split('-', 1)[-1].split('.')[0]  # Extracting date from the last file processed
    
    # Write the merged DataFrame to a new CSV file
    combined_filename = os.path.join(folder_path, f"{date}.gz")
    merged_df.to_csv(combined_filename, index=False, compression='gzip')
    return combined_filename


if __name__ == '__main__':
    nepse = Nepse()

    # This is necessary 
    nepse.headers['Connection'] = 'close'
    nepse.setTLSVerification(False) # This is temporary, until nepse sorts its ssl certificate problem
    company_list = nepse.getCompanyList()
    symbols = [i['symbol'] for i in company_list]

    save_dir = 'floorsheets'
    os.makedirs(save_dir, exist_ok=True)
    business_date = str(date.today() - timedelta(1))
    errs = save_floorsheet_day( nepse, symbols[:3], business_date,  save_dir, cache=False)
    print(errs)

    merged_file_path = merge_csv_files(save_dir)
    # if merged_file_path:
    #     csv_to_zip(merged_file_path)