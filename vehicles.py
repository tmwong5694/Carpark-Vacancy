from datetime import datetime
import json
import numpy as np
import pandas as pd
import re
import sys
import os
from Scraper import *
from bs4 import BeautifulSoup
from urllib.request import urlopen
from urllib.parse import urlencode
import urllib


# the_url = "https://api.data.gov.hk/v1/carpark-info-vacancy?data=vacancy&vehicleTypes=privateCar&lang=zh_TW"

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
        if not carpark.get(self.vehicle_type) in (None, np.nan):
            # Extract info from the dictionary of carpark vacancy
            vacancy_raw = carpark.get(self.vehicle_type)[0]
            vacancy = {
                "park_id": park_id,
                "body": {
                    "vehicle_type": self.vehicle_type,
                    "vacancy_type": vacancy_raw.get("vacancy_type"),
                    # Checkout what does vacancy type = "A" means in the data dict
                    "vacancy": vacancy_raw.get("vacancy"),
                    "last_update": vacancy_raw.get("lastupdate")
                }
            }
        return vacancy

    def get_basic_info(self, park_id: str) -> dict:
        """Takes in the park_id and returns the basic information of that car park"""
        carpark = pd.DataFrame(self.info).set_index("park_Id").loc[str(park_id)]
        basic_info = {
            "park_id": park_id,
            "body": {
                "name": carpark.get("name"),
                "nature": carpark.get("nature"),
                "carpark_type": carpark.get("carpark_Type"),
                "full_address": carpark.get("displayAddress"),  # Full address
                "district": carpark.get("district"),
                "latitude": carpark.get("latitude"),
                "longitude": carpark.get("longitude"),
                "contact_no": carpark.get("contactNo"),
                "opening_status": carpark.get("opening_status"),
                "facilities": carpark.get("facilities"),
                "payment_method": carpark.get("paymentMethods"),
                "creation_date": carpark.get("creationDate"),
                "modified_date": carpark.get("modifiedDate"),
                "published_date": carpark.get("publishedDate"),
                "website": carpark.get("website")
            }
        }

        rendition_urls = carpark.get("renditionUrls")
        if not rendition_urls in (None, np.nan):
            basic_info["body"]["square"] = rendition_urls.get("square")
            basic_info["body"]["thumbnail"] = rendition_urls.get("thumbnail")
            basic_info["body"]["banner"] = rendition_urls.get("banner")
            basic_info["body"]["carpark_photo"] = rendition_urls.get("carpark_photo")

        return basic_info

    def get_address(self, park_id: str) -> dict:
        """Takes in the park_id and return the address of that car park."""
        carpark = pd.DataFrame(self.info).set_index("park_Id").loc[str(park_id)]
        return_address = {
            "park_id": park_id,
            "body": {
                "full_address": carpark.get("displayAddress")
                }
            }
        if not carpark.get("address") in (None, np.nan):
            address = carpark["address"]
            return_address["body"]["unit_no"] = address.get("unitNo"),
            return_address["body"]["unit_descriptor"] = address.get("unitDescriptor"),
            return_address["body"]["floor"] = address.get("floor"),
            return_address["body"]["block_no"] = address.get("blockNo"),
            return_address["body"]["block_descriptor"] = address.get("blockDescriptor"),
            return_address["body"]["building_name"] = address.get("buildingName"),
            return_address["body"]["phase"] = address.get("phase"),
            return_address["body"]["estate_name"] = address.get("estateName"),
            return_address["body"]["village_name"] = address.get("villageName"),
            return_address["body"]["street_name"] = address.get("streetName"),
            return_address["body"]["building_no"] = address.get("buildingNo"),
            return_address["body"]["sub_district"] = address.get("subDistrict"),
            return_address["body"]["dc_district"] = address.get("dcDistrict"),
            return_address["body"]["region"] = address.get("region")
        return return_address

    def get_grace_periods(self, park_id: str) -> list:
        """Takes in the park_id and returns the grace periods of that car park."""
        carpark = pd.DataFrame(self.info).set_index("park_Id").loc[str(park_id)]
        grace_periods = []
        if not carpark.get("gracePeriods") in (None, np.nan):
            # Iterate through all the grace periods
            for period in carpark.get("gracePeriods"):
                grace_period = {
                    "park_id": park_id,
                    "body": {
                        "minutes": period.get("minutes"),
                        "remark": period.get("remark")
                    }
                }
                grace_periods.append(grace_period)
        return grace_periods

    def get_height_limits(self, park_id) -> list:
        """Takes in the park_id and returns the height limits of that car park."""
        carpark = pd.DataFrame(self.info).set_index("park_Id").loc[str(park_id)]
        height_limits = []
        if not carpark.get("heightLimits") in (None, np.nan):
            for height_limit in carpark.get("heightLimits"):
                height_limit_info = {
                    "park_id": park_id,
                    "body": {
                        "height": height_limit.get("height"),
                        "remark": height_limit.get("remark")
                    }
                }
                height_limits.append(height_limit_info)
        return height_limits

    def get_opening_hours(self, park_id: str) -> list:
        """Takes in the park_id and returns the opening hours of that car park."""
        carpark = pd.DataFrame(self.info).set_index("park_Id").loc[str(park_id)]
        opening_hours = []
        if not carpark.get("openingHours") in (None, np.nan):
            # Iterate through all the opening hour
            for hour in carpark.get("openingHours"):
                opening_hour = {
                    "park_id": park_id,
                    "body": {
                        "weekdays": hour.get("weekdays"),
                        "exclude_public_holiday": hour.get("excludePublicHoliday"),
                        "period_start": hour.get("periodStart"),
                        "period_end": hour.get("periodEnd")
                    }
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
            if not carpark.get(self.vehicle_type).get(mode) in (None, np.nan):
                for charge in carpark.get(self.vehicle_type).get(mode):
                    charge_clean = {}
                    if mode == "hourlyCharges":
                        charge_clean = {
                        "park_id": park_id,
                        "body": {
                            "type": charge.get("type"),
                            "weekdays": charge.get("weekdays"),
                            "exclude_public_holiday": charge.get("excludePublicHoliday"),
                            "period_start": charge.get("periodStart"),
                            "period_end": charge.get("periodEnd"),
                            "price": charge.get("price"),
                            "usage_thresholds": charge.get("usageThresholds"),
                            "covered": charge.get("covered"),
                            "remark": charge.get("remark"),
                        }
                    }
                    elif mode == "monthlyCharges":
                        charge_clean = {
                            "park_id": park_id,
                            "body": {
                                "type": charge.get("type"),
                                "price": charge.get("price"),
                                # TODO: Check the ranges again, it needed to be refined but returned results are mostly None
                                "ranges": charge.get("ranges"),
                                "covered": charge.get("covered"),
                                "reserved": charge.get("reserved"),
                                "remark": charge.get("remark")
                            }
                        }
                    elif mode == "dayNightParks":
                        charge_clean = {
                            "park_id": park_id,
                            "body": {
                                "type": charge.get("type"),
                                "weekday": charge.get("weekdays"),
                                "exclude_public_holiday": charge.get("excludePublicHoliday"),
                                "period_start": charge.get("periodStart"),
                                "period_end": charge.get("periodEnd"),
                                "valid_until": charge.get("validUntil"),
                                "valid_until_end": charge.get("validUntilEnd"),
                                "price": charge.get("price"),
                                "covered": charge.get("covered"),
                                "remark": charge.get("remark")
                            }
                        }
                    elif mode == "priveleges":
                        charge_clean = {
                            "park_id": park_id,
                            "body": {
                                "exclude_public_holiday": charge.get("excludePublicHoliday"),
                                "period_start": charge.get("periodStart"),
                                "period_end": charge.get("periodEnd"),
                                "description": charge.get("description")
                            }
                        }
                    elif mode == "unloadings":
                        charge_clean = {
                            "park_id": park_id,
                            "body": {
                                "type": charge.get("type"),
                                "price": charge.get("price"),
                                "usage_thresholds": charge.get("usageThresholds"),
                                "remark": charge.get("remark")
                            }
                        }
                    charge_clean["body"]["space"] = carpark.get(self.vehicle_type).get("space"),
                    charge_clean["body"]["space_dis"] = carpark.get(self.vehicle_type).get("spaceDIS"),
                    charge_clean["body"]["space_ev"] = carpark.get(self.vehicle_type).get("spaceEV"),
                    charge_clean["body"]["space_unl"] = carpark.get(self.vehicle_type).get("spaceUNL")
                    charges.append(charge_clean)
        return charges


    def save_json(self, info: str, park_mode: (str, None)=None) -> pd.DataFrame:
        """Get the DataFrame of the relevant information"""

        append_list = []
        for id in self.park_ids:
            # Match method to different functions according to input value
            if info == "charges":
                self.method = self.get_charges
                # Get_charges function need to specify mode of parking
                if park_mode is None:
                    raise ValueError("Please provide the parking mode.")
                element = self.method(mode=park_mode, park_id=id)
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
                    self.method = self.address
                element = self.method(park_id=id)
                
            append_list.append(element)
            # These two functions return dict, append instead of extend
            # if info in ("basic_info", "address", "vacancy"):
                # append_list.append(element)
            # These functions return list, extend instead of append
            # else:
                # append_list.extend(element)
        append_list = json.dumps(append_list)


        folder_path = "./data"
        os.makedirs(folder_path, exist_ok=True)

        with open(os.path.join(folder_path, info) + ".json", "w") as file:
            json.dump(append_list, file, indent=4)

        # return pd.DataFrame(append_list)
        return append_list

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
    pc = CarparkScraper(vehicle_type="privateCar")
    # Get data before retrieving data
    pc.get_data(data="info")
    # # Get the df for different data inquired
    # charges = pc.get_df(info="charges", park_mode="hourlyCharges")
    # grace_periods = pc.get_df(info="basic_info")
    # Get vacancy before retrieving data
    pc.get_data(data="vacancy")
    vacancy = pc.save_json(info="vacancy")
    # data = pc.save_json(info="basic_info")

    # print(vacancy.info())
    # print(vacancy.describe())

    with open("./data/vacancy.json", "r") as f:
        string = json.load(f)

    pass