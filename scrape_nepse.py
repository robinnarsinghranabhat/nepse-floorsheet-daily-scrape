import pandas as pd
from datetime import date, timedelta
import os
from nepse import Nepse

def save_floorsheet(symbol, date, save_dir, cache):
    file_name = f'./{save_dir}/{symbol}-{date}.csv'
    if cache and os.path.exists(file_name):
        return
    try:
        data = nepse.getFloorSheetOf(symbol=symbol, business_date=date)
        if not data:
            return "Empty"
        data = pd.DataFrame(data)
        print(file_name)
        print(len(data))
        data.to_csv(file_name, index=False)
    except Exception as e:
        return {"ERROR" : e, 'symbol': symbol, 'date': date}


def save_floorsheet_day(symbols, date, save_dir, cache=True):
    
    erred = []
    for symbol in symbols:
        print(f"Symbol : {symbol} - DATE : {date}")
        out = save_floorsheet(symbol, date, save_dir,cache)
        if out:
            erred.append(out) 
    return erred

def merge_csv_files(folder_path):
    # Check if the folder exists
    if not os.path.exists(folder_path):
        print(f"Folder '{folder_path}' not found.")
        return
    
    # Initialize an empty DataFrame to store the merged data
    merged_df = pd.DataFrame()
    filename = None
    # Iterate through each file in the folder
    for filename in os.listdir(folder_path):
        # Check if the file is a CSV file
        if filename.endswith('.csv'):
            # Read the CSV file into a DataFrame
            file_path = os.path.join(folder_path, filename)
            df = pd.read_csv(file_path)
            # Append the data to the merged DataFrame
            merged_df = merged_df.append(df, ignore_index=True)
            # Remove the individual CSV file
            os.remove(file_path)
    if not filename:
        print("Data not available")
        return
    # Get the date from the first file name
    date = filename.split('-', 1)[-1].split('.')[0]  # Extracting date from the last file processed
    
    # Write the merged DataFrame to a new CSV file
    combined_filename = os.path.join(folder_path, f"{date}.gz")
    merged_df.to_csv(combined_filename, index=False, compression='gzip')
    return combined_filename

import zipfile
from pathlib import Path

def csv_to_zip(csv_file_path):
    print("Converting CSV TO ZIP")
    csv_file_path = Path(csv_file_path)
    zip_file_path = f'./floorsheets/{csv_file_path.stem}.zip'
    with zipfile.ZipFile(zip_file_path, 'w') as zipf:
        # Add the CSV file to the zip file
        zipf.write(zip_file_path, os.path.basename(csv_file_path))
    os.remove(csv_file_path)

nepse = Nepse()

# This is necessary 
nepse.headers['Connection'] = 'close'
nepse.setTLSVerification(True) # This is temporary, until nepse sorts its ssl certificate problem
company_list = nepse.getCompanyList()
symbols = [i['symbol'] for i in company_list]



save_dir = 'floorsheets'
os.makedirs(save_dir, exist_ok=True)
business_date = str(date.today() - timedelta(1))
errs = save_floorsheet_day( symbols[:3], business_date,  save_dir, cache=False)
print(errs)

merged_file_path = merge_csv_files(save_dir)
# if merged_file_path:
#     csv_to_zip(merged_file_path)