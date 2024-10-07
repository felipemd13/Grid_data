#%%
import requests
from aux_functions import save_pdf, months, save_csv
import os
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor
import pdfplumber
import pandas as pd


def main():
    print('Begin scraping')
    day = 1
    month = 1
    year = 2024
    dates_to_scrape = []
    # get the days from the January from the last 3 years
    for j in range(11):
        first_date = date(year - j, month, day)
        for i in range(31):
            dates_to_scrape.append(first_date + timedelta(days=i))

    # with ThreadPoolExecutor(max_workers=10) as executor:
    #     executor.map(lambda date_to_scrape: get_pdf(date_to_scrape.day, date_to_scrape.month, date_to_scrape.year), dates_to_scrape)
    print('End scraping')
    print('-'*20)
    print('Begin parsing')
    folders = os.listdir('PDFs')
    for folder in folders:
        subfolders = os.listdir(f'PDFs/{folder}')
        for subfolder in subfolders:
            files = os.listdir(f'PDFs/{folder}/{subfolder}')
            for file in files:
                print(f'Parsing {file}')
                found_table = parse_pdf(f'PDFs/{folder}/{subfolder}/{file}')
                if not found_table:
                    print(f'Table not found in {file}')
    print('End parsing')

    # join all the CSVs
    # To each table we will add the date, year and fix the column names
    print('Begin joining CSVs')
    folders = os.listdir('CSVs')
    df_master = []
    for folder in folders:
        subfolders = os.listdir(f'CSVs/{folder}') if 'csv' not in folder else []
        for subfolder in subfolders:
            files = os.listdir(f'CSVs/{folder}/{subfolder}')
            for file in files:
                df = pd.read_csv(f'CSVs/{folder}/{subfolder}/{file}')
                df = change_column_names(df, 'Region', ['RegionRegion'])
                df = change_column_names(df, 'OD(+)/UD(-) (MU)', ['OD(+)/ UD(-) (MU)'])
                df = change_column_names(df, 'Max.Demand Met during the day (MW)', ['Max.Demand Met during the day(MW)', 'Max. Demand Met during the day (MW)'])
                df = change_column_names(df, 'Shortage during maximum Demand / Peak hour Shortage (2014) (MW)', ['Shortage during maximum Demand(MW)', 'Peak hour Shortage (MW)', 'Shortage during maximum Demand (MW)'])
                df['Date_day'] = int(file.split('_')[0])
                df['Date_month'] = subfolder
                df['Date_year'] = int(folder)
                df_master = pd.concat([df_master, df]) if len(df_master) > 0 else df
    df_master = df_master[["Region", "States", "Max.Demand Met during the day (MW)", "Shortage during maximum Demand / Peak hour Shortage (2014) (MW)",
                          "Energy Met (MU)", "Drawal Schedule (MU)", "Energy Shortage (MU)", "OD(+)/UD(-) (MU)", "Max OD (MW)",
                          "Date_day", "Date_month", "Date_year"]]
    df_master.sort_values(by=['Date_year', 'Date_day'], inplace=True)
    save_csv(df_master, 'CSVs/master_table.csv')
    print('End joining CSVs')
    # mini QA to check region with states
    df_states = df_master.groupby(['Region', 'States']).size().reset_index(name='Count')
    df_states.to_csv('CSVs/region_states.csv', index=False)
    df_master.loc[df_master['States'] == 'Assam', 'Region'] = 'NER'
    df_master.loc[df_master['States'] == 'Arunachal Pradesh', 'Region'] = 'NER'
    df_master.loc[df_master['States'] == 'Bihar', 'Region'] = 'ER'
    df_master.loc[df_master['States'] == 'DVC', 'Region'] = 'ER'
    df_states = df_master.groupby(['Region', 'States']).size().reset_index(name='Count')
    df_states.to_csv('CSVs/region_states.csv', index=False)
    print(df_states)



def change_column_names(df: pd.DataFrame, correct_name: str, wrong_names: list):
    if correct_name not in df.columns:
        for wrong_name in wrong_names:
            if wrong_name in df.columns:
                df.rename(columns={wrong_name: correct_name}, inplace=True)
                return df
    return df

# function to verify if we found the title of the table
def get_word(word_number: int, words: list):
    if 'power' in words[word_number]['text'].lower():
        if words[word_number+1]['text'].lower() == 'supply':
            if words[word_number+2]['text'].lower() == 'position':
                if words[word_number+3]['text'].lower() == 'in':
                    if words[word_number+4]['text'].lower() == 'states':
                        return True
    return False


def parse_pdf(filepath: str):
    filepath_csv = filepath.replace(".pdf", ".csv").replace("PDFs", "CSVs")
    if os.path.exists(filepath_csv):
        print(f'CSV for {filepath} already exists')
        return True
    with pdfplumber.open(filepath) as pdf:
        for page_num, page in enumerate(pdf.pages):
            text = page.extract_text()
            if "power supply position in states" in text.lower():
                print(f"Table found in page {page_num} for {filepath}")
                words = page.extract_words()
                # print([word['text'] for word in words])
                for word_number in range(len(words)):
                    if get_word(word_number, words):
                        top = words[word_number]['top']
                bbox = (
                    0,
                    top,
                    min(page.width, page.bbox[2]),
                    min(page.height, page.bbox[3])
                )
                cropped_page = page.within_bbox(bbox)
                tables = cropped_page.extract_tables()

                index = text.find("Power Supply Position in States") + len("Power Supply Position in States")
                text_table = text[index:index+100].split()
                # print(text_table)
                max_rigth_table = 0
                for table_num, table in enumerate(tables):
                    df = pd.DataFrame(table[1:], columns=table[0])
                    columns_names = list(df.columns)
                    rigth_table = 0
                    for text in text_table:
                        if any(text in column for column in columns_names if column):
                            rigth_table += 1
                    if rigth_table > max_rigth_table:
                        max_rigth_table = rigth_table
                        table_index = table_num
                columns = [col.replace('\n', ' ') for col in tables[table_index][0]]
                df = pd.DataFrame(tables[table_index][1:], columns=columns)
                dict_table = df.to_dict(orient='records')
                key_region = 'Region' if 'Region' in dict_table[0].keys() else 'RegionRegion'
                region = dict_table[0][key_region] if dict_table[0][key_region] != None and dict_table[0][key_region] != '' else 'NR'
                for row in dict_table:
                    if row[key_region] == None or row[key_region] == '':
                        row[key_region] = region
                    elif "D. Transnational Exchanges" in row[key_region]:
                        dict_table.remove(row)
                    else:
                        region = row[key_region]
                df = pd.DataFrame(dict_table)
                save_csv(df, filepath_csv)
                # print(df.head())
                return True
    return False

        
def get_pdf(day: int, month_num: int, year: int):
    month = months[month_num-1]
    filepath = f'PDFs/{year}/{month}/{day}_{month}_{year}.pdf'
    if not os.path.exists(filepath):
        response = request_page(day, month_num, year)
        if response.status_code == 200:
            save_pdf(response, filepath)
            print(f'PDF for {day}/{month}/{year} succesfully downloaded')
        else:
            print(f'Error {response.status_code} in PDF for {day}/{month}/{year}')
    else:
        print(f'PDF for {day}/{month}/{year} already exists')

def request_page(day: int, month: int, year: int):
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US;q=0.8,en;q=0.7',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Cookie': 'HttpOnly; HttpOnly',
        'Pragma': 'no-cache',
        'Referer': 'https://report.grid-india.in/psp_report.php',
        'Sec-Fetch-Dest': 'iframe',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }
    month_num =str(month).zfill(2)
    month_str = months[month-1]
    day = str(day).zfill(2)

    params_url = f'{year-1}-{year}/{month_str}%20{year}/{day}.{month_num}.{year%100}'
    response = requests.get(
        f'https://report.grid-india.in/ReportData/Daily%20Report/PSP%20Report/{params_url}_NLDC_PSP.pdf',
        headers=headers,
    )
    return response

if __name__ == '__main__':
    main()
# %%
