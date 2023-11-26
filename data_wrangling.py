import requests
import json
from itertools import product
import pandas as pd
import os 
import re

ROOT = "/Users/admin/Documents/GitHub/FINAL-PROJECT-RYAN"
DATA_PATH = os.path.join(ROOT, "data")

# To get BLS API Key, visit: https://www.bls.gov/developers/home.htm 
BLS_API_KEY = "2609c7d8d9b04b4abd76737c1e3fff07"

# To get API key for xwalk, visit: https://www.huduser.gov/portal/datasets/usps_crosswalk.html
XWALK_API_KEY = """eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6IjE4M2FhZjhjMzdlMmQwZDhiYz
                kxNDM4YWFmODE4Yzg0YTVjYzIxNGZmZmFjMjQ0NTgyNmFkN2MxMzNiMGIxZTM1NmU2NmM2YTY
                yZmRhYTFhIn0.eyJhdWQiOiI2IiwianRpIjoiMTgzYWFmOGMzN2UyZDBkOGJjOTE0MzhhYWY4
                MThjODRhNWNjMjE0ZmZmYWMyNDQ1ODI2YWQ3YzEzM2IwYjFlMzU2ZTY2YzZhNjJmZGFhMWEiL
                CJpYXQiOjE3MDA5NjA1NjQsIm5iZiI6MTcwMDk2MDU2NCwiZXhwIjoyMDE2NTc5NzY0LCJzdW
                IiOiI2MjI2OSIsInNjb3BlcyI6W119.F3_hNlfRjkFk9GcWLnrbxljjR-eWyCTaZLkGgSTEsv
                ZRSOTbGSn0Wk3vC6PQvNsotVfjTpBhdxN0yvNnw9rFYQ"""
XWALK_API_KEY = re.sub("\s+", "", XWALK_API_KEY)

STATE_DICT = {
    "01": "Alabama",
    "02": "Alaska",
    "04": "Arizona",
    "05": "Arkansas",
    "06": "California",
    "08": "Colorado",
    "09": "Connecticut",
    "10": "Delaware",
    "11": "District of Columbia",
    "12": "Florida",
    "13": "Georgia",
    "15": "Hawaii",
    "16": "Idaho",
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
    "28": "Mississippi",
    "29": "Missouri",
    "30": "Montana",
    "31": "Nebraska",
    "32": "Nevada",
    "33": "New Hampshire",
    "34": "New Jersey",
    "35": "New Mexico",
    "36": "New York",
    "37": "North Carolina",
    "38": "North Dakota",
    "39": "Ohio",
    "40": "Oklahoma",
    "41": "Oregon",
    "42": "Pennsylvania",
    "44": "Rhode Island",
    "45": "South Carolina",
    "46": "South Dakota",
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
    "66": "Guam",
    "69": "Northern Mariana Islands",
    "60": "American Samoa"
}




# Load annual state-level injury statistics of hospital workers from BLS
# For the first-time users, the function makes API call to create a csv file
def load_bls(api_key, data_path):
    data_directory = os.path.join(data_path, "bls.csv")

    if os.path.exists(data_directory):
        return pd.read_csv(data_directory)
    else:
        # For BLS API call, create a list dictionaries 
        metric_dict = {3: "MSDs P10K", #MSDs: Musculo-Skeletal Disorders Per 10K Full-time employees
                       7: "Median Days Lost From MSDs"} #Median days employees were absent to heal MSDs
        industry_dict = {"622XXX":"Hospitals"}
        
        series_data = [
            {
                "SeriesID": f"CSUMSD{industry}{metric}31{fips}",
                "Industry": industry_dict[industry],
                "Metric": metric_dict[metric],
                "State": STATE_DICT[fips],
                "StateFIPS": int(fips)
            }
            for industry, metric, fips in product(industry_dict.keys(), metric_dict.keys(), STATE_DICT.keys())
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
        
        df_bls.to_csv(os.path.join(data_path, "bls.csv"), index=False)
        return df_bls
    
    
def load_osha(data_path):
    # Append OSHA data which spans from year 2016 to 2022
    appended_df = pd.DataFrame()
    d_type = {"ein": "string", 
              "naics_code":"string", 
              "establishment_type":"string", 
              "zip_code":"object"}
    
    for year in range(2016,2023):
        file_path = os.path.join(data_path, f"ita_{year}.csv")
        current_df = pd.read_csv(file_path, encoding="Latin", dtype=d_type)
        appended_df = pd.concat([current_df, appended_df])

    # Keep rows that describe establishment with NAICS code == 61|, 622XXX (hospitals)
    df = appended_df.loc[appended_df["naics_code"].str.startswith(("61", "62")),]
    # Keep private establishment only
    df = df.loc[df["establishment_type"].str.startswith("1"),]

    # For later merge:
    df["zip_code"] = df["zip_code"].astype("string").str[:5]
    df["zip_code"] = df["zip_code"].astype("int64")

    for var in ["naics_code", "establishment_type"]:
        df[var] = df[var].str.replace(".00", "", regex=False)

    
    rename_dict = {
        "state": "State Abb",
        "naics_code": "NAICS Code",
        "zip_code" : "Zip Code",
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
    df["State Abb"] = df["State Abb"].str.upper()

    # Normalize work hour and injury stats by number of employees
    aggregate_stats = ["Deaths", "DAFW Cases", "DAFW", "Injuries", "Skin Disorders", "Resp Conditions"]
    for stat in aggregate_stats:
        df[f"{stat} P10K"] = df[f"Total {stat}"]/df["Average Employees"] * 10000

    #  Drop observations that suffer non-sampling error
    #  Note: The criteria were suggested by OSHA 
    df["Quality Flag"] = 0

    # Rule 1: Avg work hours/employee need to be between 500 and 8760 hours
    df["Avg Annual Work Hrs"] = df["Total Work Hours"]/df["Average Employees"]
    df.loc[~df["Avg Annual Work Hrs"].between(500, 8760), "Quality Flag"] = 1

    # Rule 2: total_dafw_days >= total_dafw_cases
    df.loc[~(df["Total DAFW"] >= df["Total DAFW Cases"]) | ((df["Total DAFW"] > 0) & (df["Total DAFW Cases"] == 0)), "Quality Flag"] = 1
    df = df.loc[df["Quality Flag"] == 0,]
    df.drop(columns=["Quality Flag"], inplace=True)

    # Fix corner case
    df.loc[df["State Abb"]=="MH", "State Abb"] = "MN"
 
    return df

def load_covid_data(data_path):
    df=pd.read_csv(os.path.join(data_path, "covid-19-data", "us-counties.csv"))

    # NYT report data from New York, Kings, Queens, Bronx and Richmond Counties as "New York City"
    # For cases in "New York City", FIPS code is left empty by NYT
    # We fill-in the fips code for NYC as 888888 for later merge
    df.loc[df["county"]=="New York City","fips"] = 888888

    for col_name in df.columns:
        if col_name != "fips":
            df.rename(columns={col_name: col_name.title()}, inplace=True)
        else:
            df.rename(columns={col_name: "CountyFIPS"}, inplace=True)

    # Keep the cumulative totals as of December 31st only
    df["Date"] = pd.to_datetime(df["Date"])
    df["Year"] = df["Date"].dt.year
    df["Day of Year"] = df["Date"].dt.day_of_year
    last_day_idx = df.groupby(["CountyFIPS","Year"])["Day of Year"].idxmax()
    df = df.loc[last_day_idx]

    # Calculate annual number of new covid cases for each county
    df["Annual Covid Cases"] = df.groupby("CountyFIPS")["Cases"].diff().fillna(df["Cases"])
    df["Annual Covid Deaths"] = df.groupby("CountyFIPS")["Deaths"].diff().fillna(df["Deaths"])

    df.drop(columns=["Day of Year", "Date", "Cases", "Deaths"], inplace=True)
    df["CountyFIPS"] = df["CountyFIPS"].astype("int64")
    return df

def load_xwalk(api_key, data_path):
    xwalk_directory = os.path.join(data_path, "xwalk.csv")

    if os.path.exists(xwalk_directory):
        return pd.read_csv(xwalk_directory,dtype={"Zip Code":"int64", "CountyFIPS":"int64"})
    else:
        # xwalk_type = 2 -> ZIP code to County FIPS
        xwalk_type = 2

        url = f"https://www.huduser.gov/hudapi/public/usps?type={xwalk_type}&query=All"
        headers = {"Authorization": "Bearer {0}".format(api_key)}

        response = requests.get(url, headers = headers).json()
        df_xwalk = pd.DataFrame(response["data"]["results"])
        rename_dict = {"geoid": "CountyFIPS", "zip":"Zip Code"}
        df_xwalk.rename(columns=rename_dict, inplace=True)
        for code in ["Zip Code", "CountyFIPS"]:
            df_xwalk[code] = df_xwalk[code].astype("int64")
        df_xwalk.to_csv(xwalk_directory,index=False)
        return df_xwalk

def merge_all(data_path, df_bls, df_osha, df_nyt, df_xwalk):
    # df osha has 202,092 
    # We lose about 600 entities
    df_osha = df_osha.reset_index(drop=True)
    df_osha["city"] = df_osha["city"].str.upper()
    df_osha = df_osha.merge(df_xwalk, how="inner", left_on=["Zip Code"], right_on =["Zip Code"])

    # We assume that the hopsital is located in a county that intersects with zip code & has the highest ratio of business addresses
    df_osha_ready = df_osha.loc[df_osha.groupby("id")["bus_ratio"].idxmax()] 

    # Merge Covid cases from NYT
    df=df_osha_ready.merge(df_nyt, on=["CountyFIPS", "Year"], how="left", indicator=True)

    # Merge New York City (look readme from NYT github)
    nyc_idx = (df["_merge"]=="left_only")&(df["State Abb"]=="NY")
    df_nyc = df.loc[nyc_idx]
    df_nyc["CountyFIPS"] = 888888 # Fake FIPS code for NYC merging
    df_nyc.drop(columns=["County", "State", "Annual Covid Cases", "Annual Covid Deaths"], inplace=True)
    df_nyc = df_nyc.merge(df_nyt, on=["CountyFIPS", "Year"])
    df = pd.concat([df.loc[~nyc_idx], df_nyc])

    # No covid cases before the pandemic
    covid_vars = ["Cases", "Deaths"]
    for var in covid_vars:
        pre_covid = df["Year"]<2020
        df.loc[pre_covid, f"Annual Covid {var}"] = df.loc[pre_covid, f"Annual Covid {var}"].fillna(0) 
        
    df.drop(columns=["_merge"], inplace=True)

    # Fill in U.S. state name for year < 2020
    state_xwalk = df.groupby("State Abb")["State"].agg(lambda x:x.mode())
    state_xwalk["GU"] = "Guam"
    df["State"] = df.apply(lambda row : state_xwalk[row["State Abb"]], axis = 1)
    df_merged = df.merge(df_bls, on=["State", "Year"], how="left", indicator=True)

    # Export the final dataframe
    state_vars = ["Year", "State", "State Abb", "MSDs P10K", "Median Days Lost From MSDs"]
    hospital_location = ["CountyFIPS", "Establishment Name", "EID", "NAICS Code", "Street Address"]
    hospital_employment = ["Average Employees", "Total Work Hours", "Avg Annual Work Hrs"]
    injury_type = ["DAFW Cases", "Injuries", "Resp Conditions", "Skin Disorders"]
    hospital_injuries = [f"Total {var}" for var in injury_type] + [f"{var} P10K" for var in injury_type]

    my_vars = state_vars + hospital_location + hospital_employment + hospital_injuries
    print(my_vars)
    df_final = df_merged[my_vars]
    df_final.to_csv(os.path.join(data_path, "final_data.csv"), index=False)

    return df_final

df_bls = load_bls(api_key=BLS_API_KEY, data_path=DATA_PATH)
df_osha = load_osha(DATA_PATH)
df_nyt = load_covid_data(DATA_PATH)
df_xwalk = load_xwalk(api_key=XWALK_API_KEY, data_path=DATA_PATH)
df_final = merge_all(data_path=DATA_PATH, 
                     df_bls=df_bls, 
                     df_osha=df_osha, 
                     df_nyt=df_nyt, 
                     df_xwalk=df_xwalk)