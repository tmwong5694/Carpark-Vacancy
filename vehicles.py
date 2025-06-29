from datetime import datetime
import json
import numpy as np
import pandas as pd
import re
import sqlite3
import sys
import os
from Scraper import *
from bs4 import BeautifulSoup
from urllib.request import urlopen
from urllib.parse import urlencode
import urllib


class CarparkScraper(Scraper):
    def __init__(self, vehicle_type: str, lang: str="zh_TW"):
        self.vehicle_type = vehicle_type
        self.lang = lang
        self.info = self.get_data(data="info")
        self.vacancy = None
        self.park_ids = None


    def check_input(self, data: str, vehicle_type: str, lang: str) -> None:
        """Validate the input of params"""
        input_choice = {
            "data": ("info", "vacancy"),
            "vehicleTypes": ("privateCar", "LGV", "HGV", "CV", "coach", "motorCycle"),
            "lang": ("en_US", "zh_TW", "zh_CN")
        }
        if not data in input_choice["data"]:
            raise ValueError(f"Input should be one of {input_choice['data']}. Got {data}.")
        if not vehicle_type in input_choice["vehicleTypes"]:
            raise ValueError(f"Input should be one of {input_choice['vehicleTypes']}. Got {vehicle_type}.")
        if not lang in input_choice["lang"]:
            raise ValueError(f"Input should be one of {input_choice['lang']}. Got {lang}.")

    def get_data(self, data: str="info", lang: (str, None)="zh_TW", carpark_id=None, extent=None) -> list:
        """
        https://api.data.gov.hk/v1/carpark-info-vacancy?data=<param>&vehicleTypes=<param>&carparkIds=<param>&extent=<param>&lang=<param>
        data = "info" or "vacancy"
        vehicleTypes = "privateCar", "LGV", "HGV", "CV", "coach", "motorCycle"
        lang = "en_US", "zh_TW", "zh_CN"
        """
        # Check if the data type is valid
        self.check_input(data=data, vehicle_type=self.vehicle_type, lang=lang)

        params = {
            "data": data,
            "vehicleTypes": self.vehicle_type,
            "lang": lang
        }
        if not carpark_id is None:
            params["carparkIds"] = carpark_id
        if not extent is None:
            params["extent"] = extent

        base_url = "https://api.data.gov.hk/v1/carpark-info-vacancy"

        # Encode parameters as query string
        query_string = urlencode(params)

        # Construct full URL with query parameters
        full_url = f"{base_url}?{query_string}"

        try:
            with urlopen(full_url) as response:
                response_data = response.read().decode("utf-8")
                response_data = json.loads(response_data)  # Parse the response data
            print("Response code:", response.getcode())
        except urllib.error.HTTPError as HTTPError:
            print(f"Error: Unable to connect to the API:\n{HTTPError}")

        results = response_data["results"]
        if data == "info":
            self.info = results
        elif data == "vacancy":
            results = pd.DataFrame(results).loc[: , ["park_Id", self.vehicle_type]]
            self.vacancy = results
        self.park_ids = pd.DataFrame(results)["park_Id"].unique()
        return results

    def get_vacancy(self, park_id: str) -> dict:
        """Takes in the park_id returns the vacancy information of that car park"""
        carpark = pd.DataFrame(self.vacancy).set_index("park_Id").loc[str(park_id)]
        # Return none if vacancy for the vehicle type is unavailable
        vacancy = None
        if not carpark.get(self.vehicle_type) is np.nan:
            # Extract info from the dictionary of carpark vacancy
            vacancy_raw = carpark.get(self.vehicle_type)[0]
            vacancy = {
                "park_id": park_id,
                "vehicle_type": self.vehicle_type,
                "vacancy_type": vacancy_raw.get("vacancy_type"),
                # Checkout what does vacancy type = "A" means in the data dict
                "vacancy": vacancy_raw.get("vacancy"),
                "last_update": vacancy_raw.get("lastupdate")
            }
        return vacancy

    def get_basic_info(self, park_id: str) -> dict:
        """Takes in the park_id and returns the basic information of that car park"""
        carpark = pd.DataFrame(self.info).set_index("park_Id").loc[str(park_id)]
        basic_info = {
            "park_id": park_id,
            "name": carpark.get("name"),
            "nature": carpark.get("nature"),
            "carpark_type": carpark.get("carpark_Type"),
            "full_address": carpark.get("displayAddress"),  # Full address
            "district": carpark.get("district"),
            "latitude": carpark.get("latitude"),
            "longitude": carpark.get("longitude"),
            "contact_no": carpark.get("contactNo"),
            "opening_status": carpark.get("opening_status"),
            "facilities": str(carpark.get("facilities")),
            "payment_method": str(carpark.get("paymentMethods")),
            "creation_date": carpark.get("creationDate"),
            "modified_date": carpark.get("modifiedDate"),
            "published_date": carpark.get("publishedDate"),
            "website": carpark.get("website")
        }

        rendition_urls = carpark.get("renditionUrls")
        if not rendition_urls is np.nan:
            basic_info["square"] = rendition_urls.get("square")
            basic_info["thumbnail"] = rendition_urls.get("thumbnail")
            basic_info["banner"] = rendition_urls.get("banner")
            basic_info["carpark_photo"] = rendition_urls.get("carpark_photo")
        basic_info = [basic_info]
        return basic_info

    def get_address(self, park_id: str) -> dict:
        """Takes in the park_id and return the address of that car park."""
        carpark = pd.DataFrame(self.info).set_index("park_Id").loc[str(park_id)]
        return_address = {
            "park_id": park_id,
            "full_address": carpark.get("displayAddress")
        }
        if not carpark.get("address") is np.nan:
            address = carpark["address"]
            unit_no = address.get("unitNo")
            if isinstance(unit_no, tuple):
                unit_no = unit_no[0]
            unit_descriptor = address.get("unitDescriptor")
            if isinstance(unit_descriptor, tuple):
                unit_descriptor = unit_descriptor[0]
            floor = address.get("floor")
            if isinstance(floor, tuple):
                floor = floor[0]
            block_no = address.get("blockNo")
            if isinstance(block_no, tuple):
                block_no = block_no[0]
            block_descriptor = address.get("blockDescriptor")
            if isinstance(block_descriptor, tuple):
                block_descriptor = block_descriptor[0]
            building_name = address.get("buildingName")
            if isinstance(building_name, tuple):
                building_name = building_name[0]
            phase = address.get("phase")
            if isinstance(phase, tuple):
                phase = phase[0]
            estate_name = address.get("estateName")
            if isinstance(estate_name, tuple):
                estate_name = estate_name[0]
            village_name = address.get("villageName")
            if isinstance(village_name, tuple):
                village_name = village_name[0]
            street_name = address.get("streetName")
            if isinstance(street_name, tuple):
                street_name = street_name[0]
            building_no = address.get("buildingNo")
            if isinstance(building_no, tuple):
                building_no = building_no[0]
            sub_district = address.get("subDistrict")
            if isinstance(sub_district, tuple):
                sub_district = sub_district[0]
            dc_district = address.get("dcDistrict")
            if isinstance(dc_district, tuple):
                dc_district = dc_district[0]
            region = address.get("region")
            if isinstance(region, tuple):
                region = region[0]


            return_address["unit_no"] = unit_no
            return_address["unit_descriptor"] = unit_descriptor
            return_address["floor"] = floor
            return_address["block_no"] = block_no
            return_address["block_descriptor"] = block_descriptor
            return_address["building_name"] = building_name
            return_address["phase"] = phase
            return_address["estate_name"] = estate_name
            return_address["village_name"] = village_name
            return_address["street_name"] = street_name
            return_address["building_no"] = building_no
            return_address["sub_district"] = sub_district
            return_address["dc_district"] = dc_district
            return_address["region"] = region
            
            return_address = [return_address]
        return return_address

    def get_grace_periods(self, park_id: str) -> list:
        """Takes in the park_id and returns the grace periods of that car park."""
        carpark = pd.DataFrame(self.info).set_index("park_Id").loc[str(park_id)]
        grace_periods = []
        if not carpark.get("gracePeriods") is None:
            # Iterate through all the grace periods
            for period in carpark.get("gracePeriods"):
                grace_period = {
                    "park_id": park_id,
                    "minutes": period.get("minutes"),
                    "remark": period.get("remark")
                }
                grace_periods.append(grace_period)
        return grace_periods

    def get_height_limits(self, park_id) -> list:
        """Takes in the park_id and returns the height limits of that car park."""
        carpark = pd.DataFrame(self.info).set_index("park_Id").loc[str(park_id)]
        height_limits = []
        if not carpark.get("heightLimits") is np.nan:
            for height_limit in carpark.get("heightLimits"):
                height_limit_info = {
                    "park_id": park_id,
                    "height": height_limit.get("height"),
                    "remark": height_limit.get("remark")
                }
                height_limits.append(height_limit_info)
        # Set park_id as key and the list of height_limits as list to return
        return height_limits

    def get_opening_hours(self, park_id: str) -> list:
        """Takes in the park_id and returns the opening hours of that car park."""
        carpark = pd.DataFrame(self.info).set_index("park_Id").loc[str(park_id)]
        opening_hours = []
        if not carpark.get("openingHours") is np.nan:
            # Iterate through all the opening hour
            for hour in carpark.get("openingHours"):
                opening_hour = {
                    "park_id": park_id,
                    "weekdays": str(hour.get("weekdays")),
                    "exclude_public_holiday": hour.get("excludePublicHoliday"),
                    "period_start": hour.get("periodStart"),
                    "period_end": hour.get("periodEnd")

                }
                opening_hours.append(opening_hour)
        return opening_hours

    def get_charges(self, park_id: str, mode: str) -> list:
        """Takes in the park_id and returns the charges information of that car park."""
        if not mode in ("privileges", "monthlyCharges", "hourlyCharges", "dayNightParks", "unloadings"):
            raise ValueError("Please try again with other mode of charges.")
        carpark = pd.DataFrame(self.info).set_index("park_Id").loc[str(park_id)]

        charges = []
        # Check if the carpark has the vehicle type information
        if not carpark.get(self.vehicle_type) in (None, np.nan):
            if not carpark.get(self.vehicle_type).get(mode) is None:
                for charge in carpark.get(self.vehicle_type).get(mode):
                    charge_clean = {}
                    if mode == "hourlyCharges":
                        charge_clean = {
                            "park_id": park_id,
                            "type": charge.get("type"),
                            "weekdays": str(charge.get("weekdays")),
                            "exclude_public_holiday": charge.get("excludePublicHoliday"),
                            "period_start": charge.get("periodStart"),
                            "period_end": charge.get("periodEnd"),
                            "price": charge.get("price"),
                            "usage_thresholds": charge.get("usageThresholds"),
                            "covered": charge.get("covered"),
                            "remark": charge.get("remark")
                        }
                        if not charge_clean["usage_thresholds"] is None:
                            # Convert usage_thresholds to string
                            charge_clean["usage_thresholds"] = str(charge_clean["usage_thresholds"][0])
                    elif mode == "monthlyCharges":
                        charge_clean = {
                            "park_id": park_id,
                            "type": charge.get("type"),
                            "price": charge.get("price"),
                            "ranges": charge.get("ranges"),
                            "covered": charge.get("covered"),
                            "reserved": charge.get("reserved"),
                            "remark": charge.get("remark")
                        }
                    elif mode == "dayNightParks":
                        charge_clean = {
                            "park_id": park_id,
                            "type": charge.get("type"),
                            "weekdays": str(charge.get("weekdays")),
                            "exclude_public_holiday": charge.get("excludePublicHoliday"),
                            "period_start": charge.get("periodStart"),
                            "period_end": charge.get("periodEnd"),
                            "valid_until": charge.get("validUntil"),
                            "valid_until_end": charge.get("validUntilEnd"),
                            "price": charge.get("price"),
                            "covered": charge.get("covered"),
                            "remark": charge.get("remark")
                        }
                    elif mode == "privileges":
                        charge_clean = {
                            "park_id": park_id,
                            "exclude_public_holiday": charge.get("excludePublicHoliday"),
                            "period_start": charge.get("periodStart"),
                            "period_end": charge.get("periodEnd"),
                            "description": charge.get("description")
                        }
                    elif mode == "unloadings":
                        charge_clean = {
                            "park_id": park_id,
                            "type": charge.get("type"),
                            "price": charge.get("price"),
                            "usage_thresholds": charge.get("usageThresholds"),
                            "remark": charge.get("remark")
                        }
                    space = carpark.get(self.vehicle_type).get("space")
                    if isinstance(space, tuple):
                        space = space[0]
                    space_dis = carpark.get(self.vehicle_type).get("spaceDIS")
                    if isinstance(space_dis, tuple):
                        space_dis = space_dis[0]
                    space_ev = carpark.get(self.vehicle_type).get("spaceEV")
                    if isinstance(space_ev, tuple):
                        space_ev = space_ev[0]
                    space_unl = carpark.get(self.vehicle_type).get("spaceUNL")
                    if isinstance(space_unl, tuple):
                        space_unl = space_unl[0]
                    charge_clean["space"] = space
                    charge_clean["space_dis"] = space_dis
                    charge_clean["space_ev"] = space_ev
                    charge_clean["space_unl"] = space_unl
                    charges.append(charge_clean)
        return charges


    def get_table(self, info: str) -> pd.DataFrame:
        """Get the DataFrame of the relevant information"""

        ## Get_charges function need to specify mode of parking
        #if info == "charges" and park_mode is None:
        #    raise ValueError("Please provide the parking mode.")

        info_list = []
        for id in self.park_ids:
            print("Currently processing " + info + " of carpark : " + id)
            if info == "charges":
                park_modes = ("privileges", "monthlyCharges", "hourlyCharges", "dayNightParks", "unloadings")
                self.method = self.get_charges
                element = [] 
                for park_mode in park_modes:
                    charge = self.method(park_id=id, mode=park_mode)
                    if isinstance(charge, list):
                        element.extend(charge)
                    elif isinstance(charge, dict):
                        element.append(charge)
            else:
                if info == "vacancy":
                    self.method = self.get_vacancy
                elif info == "grace_periods":
                    self.method = self.get_grace_periods
                elif info == "height_limits":
                    self.method = self.get_height_limits
                elif info == "opening_hours":
                    self.method = self.get_opening_hours
                elif info == "basic_info":
                    self.method = self.get_basic_info
                elif info == "address":
                    self.method = self.get_address
                element = self.method(park_id=id)
            if isinstance(element, list):
                info_list.extend(element)
            elif isinstance(element, dict):
                info_list.append(element)

        # Define the datatype
        datatype = {
            "park_id": "str",
            "type": "category",
            "weekdays": "string",
            "exclude_public_holiday": bool,
            "period_start": "string",
            "period_end": "string",
            "price": "Int64",
            "usage_threshold": "string",
            "covered": "category",
            "remark": "string",
            "space": "Int64",
            "space_dis": "Int64",
            "space_ev": "Int64",
            "space_unl": "Int64",
            "description": "string",
            "ranges": "string",
            "reserved": "category",
            "valid_until": "category",
            "valid_until_end": "string",
            "full_address": "string",
            "floor": "category",
            "building_name": "string",
            "street_name": "string",
            "building_no": "Int64",
            "sub_district": "string",
            "dc_district": "string",
            "region": "string",
            "height": "float16",
            "remark": "string",
            "vehicle_type": "category",
            "vacancy_type": "string",
            "vacancy": "int16",
            "last_update": "string"
        }

        dataframe = None
        if info_list:
            # Replace np.nan with None
            dataframe = pd.DataFrame(info_list).fillna(np.nan).set_index("park_id")
        # Quit the function if there is no information in that vehicle type
        else:
            return

        # Replace the NaN values in the columns of datatype string 
        for col in datatype.keys():
            if col in dataframe.columns:
                col_datatype = datatype[col]
                # Replace the NaN values with "" before turning that column into string type
                if col_datatype in ("string", "category"):
                    dataframe[col] = dataframe[col].replace(np.nan, "")
                dataframe.astype({col: datatype[col]})
        return dataframe

    def save_sqlite(self, destination: str, info: str) -> None:
        """Saved the dataframe in the sqlite database in the destination"""

        dataframe = self.get_table(info=info)
        # Do not save if the vehicle type has no such information
        if dataframe is None:
            return

        # Make the desired destination and continue if the desired destination already exists
        os.makedirs(destination, exist_ok=True)
        # Create a sqlite table and write with the corresponding table name
        db_name = os.path.join(destination, f"{self.vehicle_type}.db")
        with sqlite3.connect(db_name) as conn:
            dataframe.to_sql(info, conn, if_exists='replace', index=False)

    def save_excel(self, destination: str, info:str):
        """Saved the dataframe in the csv in the destination"""

        dataframe = self.get_table(info=info)
        # Do not save if the vehicle type has no such information
        if dataframe is None:
            return
        # Make the desired destination and continue if the desired destination already exists
        os.makedirs(destination, exist_ok=True)
        # Create a csv file and write with the corresponding table name
        csv_name = os.path.join(destination, f"{info}.csv")
        dataframe.to_csv(csv_name, index=True, encoding="utf-8-sig")

def get_public_holiday() -> np.ndarray:
    # Define a ph_dict to hold all the public holidays
    holiday_url = "https://www.gov.hk/en/about/abouthk/holiday/2025.htm"
    html_content = Scraper(url=holiday_url, decode="utf-8").openurl()
    soup = BeautifulSoup(html_content, features="html.parser")
    table = soup.find_all("table")[0]
    table_rows = table.find_all("tr")

    def get_table_data(table_row_input):
        """Get the table data from a table row"""
        table_data = table_row_input.find_all("td")
        name, date, weekday = [*map(lambda x: x.get_text(strip=True), table_data)]
        return np.array((name, date, weekday), dtype="U50")

    ph_array = np.asarray([*map(get_table_data, table_rows)], dtype="U50")

    def convert_date(x):
        """Convert date string to datetime object"""
        if x != "":
            return datetime.strptime(x, "%d %B").replace(year=2025)
        else:
            return x
    vectorized_convert_date = np.vectorize(convert_date)

    mask = ph_array[:, 1] != ""
    ph_array[:, 1][mask] = vectorized_convert_date(ph_array[:, 1][mask])

    return ph_array

if __name__ == "__main__":
    # Initialize the vehicle type
    pc = CarparkScraper(vehicle_type="LGV")
    # Get data before retrieving data
    pc.get_data(data="info")
    # Get vacancy before retrieving data
    pc.get_data(data="vacancy")
    
    info_list = ["address", "basic_info", "height_limits", "opening_hours", "grace_periods", "vacancy", "charges"]
    folder_path = "./data"
    for info in info_list:
        table = pc.save_excel(info=info, destination=folder_path)
    
    pass