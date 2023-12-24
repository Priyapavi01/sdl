import pandas as pd
import os
import shutil
from datetime import datetime
from datetime import date

# Define paths
# input_folder = r"C:\Users\e600294\Documents\SDL\01_input"
# lookup_file_path = r"C:\Users\e600294\Documents\SDL\02_lookup\Source Nature Categorization_SPI NA Scheduling.xlsx"
# validation_path = r"C:\Users\e600294\Documents\SDL\03_report_to_validation"
# db_path = r"C:\Users\e600294\Documents\SDL\04_main_db"
# completed_path = r"C:\Users\e600294\Documents\SDL\05_completed_files"
# validation_file = os.path.join(validation_path, "missing_source_ids.csv")
#Define paths
# input_folder = r"C:\Users\priya\Documents\SDL\01_input"
# lookup_file_path = r"C:\Users\priya\Documents\SDL\02_lookup\Source Nature Categorization_SPI NA Scheduling.xlsx"
# validation_path = r"C:\Users\priya\Documents\SDL\03_report_to_validation"
# db_path = r"C:\Users\priya\Documents\SDL\04_main_db"
# completed_path = r"C:\Users\priya\Documents\SDL\05_completed_files"
# validation_file = os.path.join(validation_path, "missing_source_ids.csv")
# Define paths
input_folder = r"C:\Users\e600294\Xperi\Premkumar Jaganathan (CTR) - sdl_report\01_input"
lookup_file_path = r"C:\Users\e600294\Xperi\Premkumar Jaganathan (CTR) - sdl_report\02_lookup\Source Nature Categorization_SPI NA Scheduling.xlsx"
validation_path = r"C:\Users\e600294\Xperi\Premkumar Jaganathan (CTR) - sdl_report\03_report_to_validation"
db_path = r"C:\Users\e600294\Xperi\Premkumar Jaganathan (CTR) - sdl_report\04_main_db"
completed_path = r"C:\Users\e600294\Xperi\Premkumar Jaganathan (CTR) - sdl_report\05_completed_files"
validation_file = os.path.join(validation_path, "missing_source_ids.csv")


# Check if file exists
if os.path.isfile(validation_file):
    os.remove(validation_file)  # Remove (delete) the file

sheet_names = ["SPI NA APAC Scheduling Team", "SPI NA Ingest Team", "SPi NA Scheduling Team"]
current_year = date.today().year

# Load lookup data
lookup_df = pd.read_excel(lookup_file_path, sheet_name='db')
lookup_source_ids = lookup_df['Source ID'].unique()

# Check if main_db exists
main_db_filename = 'main_db.xlsx'
main_db_path = os.path.join(db_path, main_db_filename)
if os.path.isfile(main_db_path):
    main_db_df = pd.read_excel(main_db_path)
    # Check if 'Report Date' column exists in main_db_df
    if 'Report Date' not in main_db_df.columns:
       raise ValueError("'Report Date' column does not exist in main_db_df")
    
else:
    main_db_df = pd.DataFrame()

# Get unique report dates from main db
existing_dates = pd.to_datetime(main_db_df['Report Date']).dt.date.unique()

# Process files
files = os.listdir(input_folder)
if not files:
    raise ValueError("No files found in the input folder")

for filename in files:
    if filename.endswith(".xlsx"):
        file_path = os.path.join(input_folder, filename)
        date_str = filename.split('_')[0]
        report_date = datetime.strptime(f"{date_str}-{current_year}", "%m-%d-%Y").date()

        # Validate if report_date already exists in main db
        if report_date in existing_dates:
            print(f"The file '{filename}' has a report date that already exists in the main database. Skipping this file.")
            continue

        dfs = {sheet_name: pd.read_excel(file_path, sheet_name=sheet_name) for sheet_name in sheet_names}
        for df in dfs.values():
            df.insert(0, 'Report Date', report_date)

        # Concatenate all data from different sheets
        all_data = pd.concat(dfs.values(), ignore_index=True)
        all_data.drop_duplicates(subset=all_data.columns.difference(['Team','Reporter']), keep='first', inplace=True)
        

        # If any of the specified columns have different values for the same 'Source ID', keep all data
        unique_all = all_data.drop_duplicates('Source ID', keep=False)

        duplicates_all = all_data[all_data.duplicated('Source ID', keep=False)]
        final_duplicates_list = []

        for name, group in duplicates_all.groupby('Source ID'):
            if not group[['Lines\nTouched', 'Days\nLogged', 'Line\nMulti-plier', 'Line\nWork\nUnits', 'Day\nWork\nUnits', 'Total\nWork\nUnits']].equals(group[['Lines\nTouched', 'Days\nLogged', 'Line\nMulti-plier', 'Line\nWork\nUnits', 'Day\nWork\nUnits', 'Total\nWork\nUnits']].iloc[0]):
                # If any of the specified columns have different values, keep all data
                final_duplicates_list.append(group)
            else:
                # If all specified columns have the same values, you can choose to skip or keep one record
                # For simplicity, I'll keep one record here
                final_duplicates_list.append(group.head(1))
        
        # Check if final_duplicates_list is not empty before concatenating
        if final_duplicates_list:
            final_duplicates = pd.concat(final_duplicates_list, ignore_index=True)
        else:
            final_duplicates = pd.DataFrame()  # or handle it according to your needs
        
        all_unique_entries = pd.concat([unique_all, final_duplicates], ignore_index=True)

        all_unique_entries = pd.concat([unique_all, final_duplicates], ignore_index=True)

        # Merge on Source ID
        all_unique_entries = pd.merge(all_unique_entries, lookup_df[['Source ID', 'Logging Pattern / Source Nature', 'Source Type']], on='Source ID', how='left')

        # Perform validation on source ids
        missing_ids = all_unique_entries[~all_unique_entries['Source ID'].isin(lookup_source_ids)]
        if len(missing_ids) > 0:
            # Export data with missing source id to validation_path
            missing_ids[['Source ID','Source Name']].to_csv(os.path.join(validation_path, "missing_source_ids.csv"), index=False)
            print("Some Source IDs are missing in the lookup. Please check missing_source_ids.csv file in the validation folder.")
            continue

        # Append new data to the main db
        main_db_df = pd.concat([main_db_df, all_unique_entries], ignore_index=True)
        main_db_df = main_db_df.drop(main_db_df[(main_db_df['Logging Pattern / Source Nature'] == "Blank")].index)

        # Write the result to an Excel file
        main_db_df.to_excel(main_db_path, index=False)

        # Move processed files into completed_path
        destination = os.path.join(completed_path, filename)
        shutil.move(file_path, destination)
