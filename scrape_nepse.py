import pandas as pd
from datetime import date, timedelta
import os
from nepse import Nepse
import time
import random


def user_agents_generator(file_path='user_agents.txt', chunk_size=200):
    with open(file_path, 'r') as file:
        while True:
            chunk = [next(file).strip() for _ in range(chunk_size)]
            if not chunk:  # End of file
                file.seek(0)  # Move cursor to the beginning of the file
                continue  # Restart the loop to cycle through the file
            yield from chunk



def save_floorsheet(symbol, date, save_dir, user_gen, cache):

    nepse = Nepse()
    nepse.headers['Connection'] = 'close'
    nepse.headers['User-Agent'] = next(user_gen)
    nepse.setTLSVerification(False) # This is temporary, until nepse sorts its ssl certificate problem

    file_name = f'./{save_dir}/{symbol}-{date}.csv'
    
    if cache and os.path.exists(file_name):
        print("Caching ! ")
        return
    try:
        time.sleep(random.uniform(5,7)) # Deliberate Throttling
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


def save_floorsheet_day(symbols, date, save_dir, user_gen, cache=True):

    if os.path.exists( f'./{save_dir}/{date}.gz' ):
        return
    

    print("Processing : ", date)

    erred = []
    for symbol in symbols:
        print(f"Symbol : {symbol} - DATE : {date}")
        out = save_floorsheet(symbol, date, save_dir,user_gen, cache)
        if out:
            erred.append(out) 
    return erred

def merge_csv_files(folder_path):
    # Check if the folder exists
    if not os.path.exists(folder_path):
        print(f"Folder '{folder_path}' not found.")
        return
    
    # Initialize an empty DataFrame to store the merged data
    dfs = []
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
            dfs.append(df)
            # Remove the individual CSV file
            os.remove(file_path)
    if not save_name:
        print("Data not available")
        return
    # Get the date from the first file name
    date = save_name.split('-', 1)[-1].split('.')[0]  # Extracting date from the last file processed
    
    # Write the merged DataFrame to a new CSV file
    combined_filename = os.path.join(folder_path, f"{date}.gz")

    merged_df = pd.concat(dfs)
    merged_df.to_csv(combined_filename, index=False, compression='gzip')
    return combined_filename


if __name__ == '__main__':
    nepse = Nepse()

    # This is necessary 
    nepse.headers['Connection'] = 'close'
    nepse.setTLSVerification(False) # This is temporary, until nepse sorts its ssl certificate problem

    # TODO : API to get companies that have been traded a given day ?  
    company_list = nepse.getCompanyList()
    symbols = [i['symbol'] for i in company_list]

    save_dir = 'daily_floorsheets'
    os.makedirs(save_dir, exist_ok=True)
    business_date = str(date.today())
    user_agents_gen = user_agents_generator()
    # errs = save_floorsheet_day( symbols[:3], business_date,  save_dir, user_agents_gen, cache=False)
    errs = save_floorsheet_day( ['JFL', 'HBL'], business_date,  save_dir, user_agents_gen, cache=False)
    print(errs)

    merged_file_path = merge_csv_files(save_dir)
    # if merged_file_path:
    #     csv_to_zip(merged_file_path)
