from datetime import datetime
import json
import numpy as np
import pandas as pd
import re
import sys
from Scraper import *
from bs4 import BeautifulSoup
from urllib.request import urlopen
import urllib


the_url = "https://api.data.gov.hk/v1/carpark-info-vacancy?data=vacancy&vehicleTypes=privateCar&lang=zh_TW"

class CarparkScraper(Scraper):
    def __init__(self, vehicle_type: str, lang: str="zh_TW"):
        self.vehicle_type = vehicle_type
        self.lang = lang
        self.info = None
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

        self.check_input(data=data, vehicle_type=self.vehicle_type, lang=lang)

        # Get response from the Carpark Vacancy API
        if carpark_id is None and extent is None:
            input_url = f"https://api.data.gov.hk/v1/carpark-info-vacancy?data={data}&vehicleTypes={self.vehicle_type}&lang={lang}"
        elif not carpark_id is None and extent is None:
            input_url = f"https://api.data.gov.hk/v1/carpark-info-vacancy?data={data}&vehicleTypes={self.vehicle_type}&carparkIds={carpark_id}&lang={lang}"
        elif carpark_id is None and not extent is None:
            input_url = f"https://api.data.gov.hk/v1/carpark-info-vacancy?data={data}&vehicleTypes={self.vehicle_type}&extent={extent}&lang={lang}"
        try:
            with urlopen(input_url) as response:
                response_data = response.read().decode("utf-8")
                response_data = json.loads(response_data)  # Parse the response data
            print("Response code:", response.getcode())
        except urllib.error.HTTPError as HTTPError:
            print(f"Error: Unable to connect to the API:\n{HTTPError}")
            exit(1)
        # results = pd.DataFrame(response_data["results"]).loc[:, ["park_Id", self.vehicle_type]]
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
            "facilities": carpark.get("facilities"),
            "payment_method": carpark.get("paymentMethods"),
            "creation_date": carpark.get("creationDate"),
            "modified_date": carpark.get("modifiedDate"),
            "published_date": carpark.get("publishedDate"),
            "website": carpark.get("website")
        }

        basic_info["grace_periods"] = None
        if not carpark.get("gracePeriods") in (None, np.nan):
            basic_info["grace_periods"] = carpark.get("gracePeriods")[0]["minutes"]

        basic_info["height_limits"] = None
        if not carpark.get("heightLimits") in (None, np.nan):
            basic_info["height_limits"] = carpark.get("heightLimits")[0]["height"]

        address = carpark.get("address")
        if not address in (None, np.nan):
            basic_info["floor"] = address.get("floor")
            basic_info["building_name"] = address.get("buildingName")
            basic_info["street_name"] = address.get("streetName")
            basic_info["building_no"] = address.get("buildingNo")
            basic_info["sub_district"] = address.get("subDistrict")
            basic_info["dc_district"] = address.get("dcDistrict")
            basic_info["region"] = address.get("region")

        rendition_urls = carpark.get("renditionUrls")
        if not rendition_urls in (None, np.nan):
            basic_info["square"] = rendition_urls.get("square")
            basic_info["thumbnail"] = rendition_urls.get("thumbnail")
            basic_info["banner"] = rendition_urls.get("banner")

        # if not carpark.get("openingHours") in (None, np.nan):
        #     opening_hours_weekdays = carpark["openingHours"][0]
        #     basic_info["weekdays_open"] = opening_hours_weekdays.get("weekdays")
        #     basic_info["exclude_public_holiday"] = opening_hours_weekdays.get("excludePublicHoliday")
        #     basic_info["period_start"] = opening_hours_weekdays.get("periodStart")
        #     basic_info["period_end"] = opening_hours_weekdays.get("periodEnd")
        return basic_info

    def get_address(self, park_id: str):
        """Takes in the park_id and return the address of that car park."""
        carpark = pd.DataFrame(self.info).set_index("park_Id").loc[str(park_id)]
        addresses = []

        return

    def get_opening_hours(self, park_id: str) -> list:
        """Takes in the park_id and returns the opening hours of that car park."""
        carpark = pd.DataFrame(self.info).set_index("park_Id").loc[str(park_id)]
        opening_hours = []
        if not carpark.get("openingHours") in (None, np.nan):
            # Iterate through all the opening hour
            for hour in carpark.get("openingHours"):
                opening_hour = {
                    "park_id": park_id,
                    "weekdays": hour.get("weekdays"),
                    "exclude_public_holiday": hour.get("excludePublicHoliday"),
                    "period_start": hour.get("periodStart"),
                    "period_end": hour.get("periodEnd")
                }
                opening_hours.append(opening_hour)
        return opening_hours

    def get_charges(self, park_id: str, mode: str):
        """Takes in the park_id and returns the charges information of that car park."""
        if not mode in ("privileges", "monthlyCharges", "hourlyCharges", "dayNightParks"):
            raise ValueError("Please try again with other mode of charges.")
        carpark = pd.DataFrame(self.info).set_index("park_Id").loc[str(park_id)]

        charge_result = None
        if not carpark.get(self.vehicle_type) in (None, np.nan):
            if not carpark.get(self.vehicle_type).get(mode) in (None, np.nan):
                charge = carpark.get(self.vehicle_type).get(mode)[0]
                charge_result = {
                    "park_id": park_id,
                    "weekdays": charge.get("weekdays"),
                    "exclude_public_holiday": charge.get("excludePublicHoliday"),
                    "period_start": charge.get("periodStart"),
                    "period_end": charge.get("periodEnd"),
                    "price": charge.get("price"),
                    "description": charge.get("description"),
                    "type": charge.get("type"),
                    "covered": charge.get("covered"),
                    "valid_until": charge.get("validUntil"),
                    "space": carpark.get("space"),
                    "space_dis": carpark.get("spaceDIS"),
                    "space_ev": carpark.get("spaceEV"),
                    "space_unl": carpark.get("spaceUNL")
                }
        return charge_result


    # def get_df(self, data: str="charges") -> pd.DataFrame:
    #     """Get the DataFrame of the relevant information"""
    #     append_list = [self.get_charges()]
    #
    #
    #     return

def get_charges_df(all_info: dict, vehicle_type: str) -> pd.DataFrame:
    """Takes in a dictionary of carpark info and returns a DataFrame of vacancy information for the specified vehicle type."""

    def get_charges(single_info: dict, charges_type: str) -> list:
        """Takes in a dictionary of carpark info and returns a list of charges information for the specified vehicle type."""
        # Get information on the weekdays and weekend charges
        car_type = single_info.get(vehicle_type)
        blank_dict = {
            "park_id": single_info.get("park_Id"),
            "name": single_info.get("name"),
            "vehicle_type": vehicle_type
        }
        if car_type is None:
            print(
                f"There is no {vehicle_type} information in the carpark info dictionary: {single_info.get('park_Id')}")
            return

        charges = None
        # Try to get hourly charges regarding this vehicle type
        if not car_type.get(charges_type) is None:
            charges = []
            # Iterate through the list of hourly_charges
            for charge in car_type.get(charges_type):
                if charges_type == "hourlyCharges":
                    charge = {
                        "park_id": single_info.get("park_Id"),
                        "name": single_info.get("name"),
                        "vehicle_type": vehicle_type,
                        "weekdays": charge.get("weekdays"),
                        "exclude_ph": charge.get("excludePublicHoliday"),
                        "remark": charge.get("remark"),
                        "usage_minimum": charge.get("usageMinimum"),
                        "covered": charge.get("covered"),
                        "type": charge.get("type"),
                        "price": charge.get("price"),
                        "period_start": charge.get("periodStart"),
                        "period_end": charge.get("periodEnd")
                    }
                elif charges_type == "monthlyCharges":
                    charge = {
                        "park_id": single_info.get("park_Id"),
                        "name": single_info.get("name"),
                        "vehicle_type": vehicle_type,
                        "covered": charge.get("covered"),
                        "price": charge.get("price"),
                        "reserved": charge.get("reserved"),
                        "type": charge.get("type")
                    }
                elif charges_type == "dayNightParks":
                    charge = {
                        "park_id": single_info.get("park_Id"),
                        "name": single_info.get("name"),
                        "vehicle_type": vehicle_type,
                        "exclude_ph": charge.get("excludePublicHoliday"),
                        "period_start": charge.get("periodStart"),
                        "period_end": charge.get("periodEnd"),
                        "price": charge.get("price"),
                        "type": charge.get("type"),
                        "covered": charge.get("covered"),
                        "valid_until": charge.get("validUntil")
                    }
                elif charges_type == "privileges":
                    print(f"Privileges is not supported for carpark {_} yet (carpark_id {single_info.get('park_Id')})")
                    break
                if not charge is None:
                    charges.append(charge)
        return charges

    info_list = []
    for _ in range(len(all_info)):
        hourly = get_charges(single_info=all_info[_], charges_type="hourlyCharges")
        monthly = get_charges(single_info=all_info[_], charges_type="monthlyCharges")
        day_night = get_charges(single_info=all_info[_], charges_type="dayNightParks")
        privileges = get_charges(single_info=all_info[_], charges_type="privileges")
        if not hourly is None:
            info_list.append(hourly)
        if not monthly is None:
            info_list.append(monthly)
        if not day_night is None:
            info_list.append(day_night)
    return_df = None
    if not len(info_list) == 0:
        return_df = pd.DataFrame(np.hstack(info_list).tolist())
    return return_df


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
    pc, mc = CarparkScraper(vehicle_type="privateCar"), CarparkScraper(vehicle_type="privateCar")
    pc.get_data(data="info")
    pc.get_data(data="vacancy")
    mc.get_data(data="info")
    mc.get_data(data="vacancy")
    mc_list = []
    basicinfo= []
    for id in mc.park_ids:
        basic_info = mc.get_basic_info(park_id=id)
        # charges = pc.get_charges(park_id=id, mode="hourlyCharges")
        # if not charges is None:
        #     pc_list.append(charges)
        opening_hours = mc.get_opening_hours(park_id=id)
        if not opening_hours is None:
            mc_list.extend(opening_hours)
        if not basic_info is None:
            basicinfo.append(basic_info)

    info_df = pd.DataFrame(basicinfo)
    # pc_charges_df = pd.DataFrame(pc_list)
    pc_opening_hours_df = pd.DataFrame(mc_list)


    print("End of program.")
