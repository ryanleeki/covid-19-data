import requests
import json
from itertools import product
import pandas as pd
import os 

ROOT = "/Users/admin/Documents/GitHub/FINAL-PROJECT-RYAN"
DATA_PATH = os.path.join(ROOT, "data")

# To get BLS API Key, visit: https://www.bls.gov/developers/home.htm 
API_KEY = "2609c7d8d9b04b4abd76737c1e3fff07"

# Load annual state-level injury statistics of hospital workers from BLS
# For the first-time users, the function makes API call to create a csv file
def load_bls(api_key, data_path):
    data_directory = os.path.join(data_path, "bls.csv")

    if os.path.exists(data_directory):
        return pd.read_csv(data_directory)
    else:
        # For BLS API call, create a list dictionaries 
        metric_dict = {3: "Injury per 10,000 full-time workers",
                       7: "Median days lost"}
        industry_dict = {"622XXX":"Hospitals"}
        
        # Note: BLS doesn't provide data for some states like Colorado 
        state_dict = {
                    "01": "Alabama",
                    "02": "Alaska",
                    "04": "Arizona",
                    "05": "Arkansas",
                    "06": "California",
                    "09": "Connecticut",
                    "10": "Delaware",
                    "11": "District of Columbia",
                    "13": "Georgia",
                    "15": "Hawaii",
                    "17": "Illinois",
                    "18": "Indiana",
                    "19": "Iowa",
                    "20": "Kansas",
                    "21": "Kentucky",
                    "22": "Louisiana",
                    "23": "Maine",
                    "24": "Maryland",
                    "25": "Massachusetts",
                    "26": "Michigan",
                    "27": "Minnesota",
                    "29": "Missouri",
                    "30": "Montana",
                    "31": "Nebraska",
                    "32": "Nevada",
                    "34": "New Jersey",
                    "35": "New Mexico",
                    "36": "New York",
                    "37": "North Carolina",
                    "39": "Ohio",
                    "40": "Oklahoma",
                    "41": "Oregon",
                    "42": "Pennsylvania",
                    "43": "Puerto Rico",
                    "45": "South Carolina",
                    "47": "Tennessee",
                    "48": "Texas",
                    "49": "Utah",
                    "50": "Vermont",
                    "51": "Virginia",
                    "52": "Virgin Islands",
                    "53": "Washington",
                    "54": "West Virginia",
                    "55": "Wisconsin",
                    "56": "Wyoming",
                    "66": "Guam"
                    }

        series_data = [
            {
                "SeriesID": f"CSUMSD{industry}{metric}31{fips}",
                "Industry": industry_dict[industry],
                "Metric": metric_dict[metric],
                "State": state_dict[fips],
                "StateFIPS": int(fips)
            }
            for industry, metric, fips in product(industry_dict.keys(), metric_dict.keys(), state_dict.keys())
        ]
        
        df_series = pd.DataFrame(series_data)

        # Note: BLS API Accepts 50 series per query
        # So we need to split up our series list for the API call
        def split_list(input_list, chunk_size=50):
            return [input_list[i:i + chunk_size] for i in range(0, len(input_list), chunk_size)]
        
        series_list = df_series["SeriesID"].to_list()
        query_list = split_list(series_list)

        csv_text = "SeriesID,Year,Value"
        for query in query_list:
            headers = {'Content-type': 'application/json'}
            data = json.dumps({"seriesid": query, "startyear":"2011", "endyear":"2020", "registrationkey": api_key})
            p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)
            json_data = json.loads(p.text)
            for series in json_data['Results']['series']:
                seriesId = series['seriesID']
                for item in series['data']:
                    year = item['year']
                    value = item['value']
                    row = f"{seriesId},{year},{value}"
                    csv_text = csv_text + "\n" + row 

        with open(os.path.join(data_path, "bls.csv"), "w") as ofile:
            ofile.write(csv_text)

        df_injury = pd.read_csv(os.path.join(data_path,"bls.csv"))
        df_bls = df_injury.merge(df_series, on="SeriesID")
        
        df_bls = df_bls.pivot(index=["State", "StateFIPS", "Year"], 
                              columns="Metric", values="Value").reset_index().rename_axis(None, axis=1)
        
        df_bls.to_csv(os.path.join(data_path, "bls.csv"))
        return df_bls
    
    
def load_osha(data_path):
    # Append OSHA data which spans from year 2016 to 2022
    appended_df = pd.DataFrame()
    d_type = {"ein": "string", "naics_code":"string", "establishment_type":"string"}
    
    for year in range(2016,2023):
        file_path = os.path.join(data_path, f"ita_{year}.csv")
        current_df = pd.read_csv(file_path, encoding="Latin", dtype=d_type)
        appended_df = pd.concat([current_df, appended_df])

    # Keep rows that describe establishment with NAICS code == 61|, 622XXX (hospitals)
    df = appended_df.loc[appended_df["naics_code"].str.startswith(("61", "62")),]
    # Keep private establishment only
    df = df.loc[df["establishment_type"].str.startswith("1"),]

    for var in ["naics_code", "establishment_type"]:
        df[var] = df[var].str.replace(".00", "", regex=False)
    
    rename_dict = {
        "state": "State",
        "naics_code": "NAICS Code",
        "establishment_name": "Establishment Name",
        "street_address": "Street Address",
        "industry_description": "Industry Description",
        "year_filing_for": "Year",
        "annual_average_employees": "Average Employees",
        "total_hours_worked": "Total Work Hours",
        "no_injuries_illnesses": "No Injury",
        "total_deaths": "Total Deaths",
        "total_dafw_cases": "Total DAFW Cases",   # Days Away From Work
        "total_dafw_days": "Total DAFW",
        "total_injuries": "Total Injuries",
        "total_skin_disorders": "Total Skin Disorders",
        "total_respiratory_conditions": "Total Resp Conditions",
        "establishment_id": "EID"
    }

    df.rename(columns=rename_dict, inplace= True)
    df["State"] = df["State"].str.upper()

    # Normalize work hour and injury stats by number of employees
    aggregate_stats = ["Deaths", "DAFW Cases", "DAFW", "Injuries", "Skin Disorders", "Resp Conditions"]
    for stat in aggregate_stats:
        df[f"{stat} P10K"] = df[f"Total {stat}"]/df["Average Employees"] * 10000

    #  Flag observations that suffer non-sampling error
    #  Note: The criteria were suggested by OSHA 
    df["Quality Flag"] = 0

    # Rule 1: Avg work hours/employee need to be between 500 and 8760 hours
    df["Avg Annual Work Hrs"] = df["Total Work Hours"]/df["Average Employees"]
    df.loc[~df["Avg Annual Work Hrs"].between(500, 8760), "Quality Flag"] = 1

    # Rule 2: total_dafw_days >= total_dafw_cases
    df.loc[~(df["Total DAFW"] >= df["Total DAFW Cases"]) | ((df["Total DAFW"] > 0) & (df["Total DAFW Cases"] == 0)), "Quality Flag"] = 1

    # Drop observations that are flagged by OSHA
    print(df["Quality Flag"].value_counts())
    
    df = df.loc[df["Quality Flag"] == 0,]
    df.drop(columns=["Quality Flag"], inplace=True)

    return df


def load_nyt_covid(data_path):
    pass

def merge_all(df_bls, df_osha, df_nyt):
    pass

df_bls = load_bls(api_key=API_KEY, data_path=DATA_PATH)
df_osha = load_osha(data_path=DATA_PATH)
print(df_bls.head())
print(df_osha.head())

