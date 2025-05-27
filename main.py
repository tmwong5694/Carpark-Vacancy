from urllib.request import urlopen
import json
import pandas as pd
import sys
sys.path.insert(1, "/Users/timwong/Desktop/Misc/Utilities")
from Scraper import *

the_url = "https://api.data.gov.hk/v1/carpark-info-vacancy?data=vacancy&vehicleTypes=privateCar&lang=zh_TW"

class Data(Scraper):
    def __init__(self, data: str="info", vehicle_types: str="privateCar", lang: str="zh_TW", carpark_ids=None, extent=None):
        self.data = data
        self.vehicle_types = vehicle_types
        self.lang = lang
        self.carpark_ids = carpark_ids
        self.extent = extent

    def get_response_data(self) -> dict:
        """
        https://api.data.gov.hk/v1/carpark-info-vacancy?data=<param>&vehicleTypes=<param>&carparkIds=<param>&extent=<param>&lang=<param>
        data = "info" or "vacancy"
        vehicleTypes = "privateCar", "LGV", "HGV", "CV", "coach", "motorCycle"
        lang = "en_US", "zh_TW", "zh_CN"
        """
        def check_type() -> None:
            """Check the type of input"""
            if not isinstance(self.data, str):
                raise TypeError(f"Input should be of string type. Got {type(self.data)}.")
            if not isinstance(self.vehicle_types, str):
                raise TypeError(f"Input should be of string type. Got {type(self.vehicle_types)}.")
            if not isinstance(self.lang, str):
                raise TypeError(f"Input should be of string type. Got {type(self.lang)}.")

        def check_input() -> None:
            """Validate the input of params"""
            input_choice = {
                "data": ["info", "vacancy"],
                "vehicleTypes": ["privateCar", "LGV", "HGV", "CV", "coach", "motorCycle"],
                "lang": ["en_US", "zh_TW", "zh_CN"]
            }
            if not self.data in input_choice["data"]:
                raise ValueError(f"Input should be one of {self.input_choice['data']}. Got {self.data}.")
            if not self.vehicle_types in input_choice["vehicleTypes"]:
                raise ValueError(f"Input should be one of {self.input_choice['vehicleTypes']}. Got {self.data}.")
            if not self.lang in input_choice["lang"]:
                raise ValueError(f"Input should be one of {self.input_choice['lang']}. Got {self.data}.")

        check_type()
        check_input()

        # Get response from the Carpark Vacancy API
        if self.carpark_ids is None and self.extent is None:
            input_url = f"https://api.data.gov.hk/v1/carpark-info-vacancy?data={self.data}&vehicleTypes={self.vehicle_types}&lang={self.lang}"
        elif not self.carpark_ids is None and self.extent is None:
            input_url = f"https://api.data.gov.hk/v1/carpark-info-vacancy?data={self.data}&vehicleTypes={self.vehicle_types}&carparkIds={self.carpark_ids}&lang={self.lang}"
        elif self.carpark_ids is None and not self.extent is None:
            input_url = f"https://api.data.gov.hk/v1/carpark-info-vacancy?data={self.data}&vehicleTypes={self.vehicle_types}&extent={self.extent}&lang={self.lang}"
        try:
            with urlopen(input_url) as response:
                response_data = response.read().decode("utf-8")
                response_data = json.loads(response_data)  # Parse the response data
            print("Response code:", response.getcode())
        except:
            print("Error: Unable to connect to the API.")
            exit(1)
        return response_data["results"]
'''
class Carpark():
    def __init__(self, info_dict: dict, vacancy_dict: dict):
        self.info_dict = info_dict
        self.vacancy_dict = vacancy_dict[]
        self.address = info_dict["address"]
        self.rendition_urls = info_dict["renditionUrls"]
        self.opening_hours_weekdays = info_dict["openingHours"][0]

        self.park_id=self.info_dict["park_Id"],
        self.name=self.info_dict["name"],
        self.nature=self.info_dict["nature"],
        self.carpark_type=self.info_dict["carpark_Type"],
        self.floor=self.address["floor"],
        self.building_name=self.address["buildingName"],
        self.street_name=self.address["streetName"],
        self.building_no=self.address["buildingNo"],
        self.sub_district=self.address["subDistrict"],
        self.dc_district=self.address["dcDistrict"],
        self.region=self.address["region"],
        self.address=self.info_dict["displayAddress"],  # Full address
        self.district=self.info_dict["district"],
        self.latitude=self.info_dict["latitude"],
        self.longitude=self.info_dict["longitude"],
        self.contact_no=self.info_dict["contactNo"],
        self.square=self.rendition_urls["square"],
        self.thumbnail=self.rendition_urls["thumbnail"],
        self.banner=self.rendition_urls["banner"],
        self.website=self.info_dict["website"],
        self.opening_status=self.info_dict["opening_status"],
        self.weekdays_open=self.opening_hours_weekdays["weekdays"],
        self.exclude_public_holiday=self.opening_hours_weekdays["excludePublicHoliday"],
        # "remark": opening_hours_weekdays["remark"],
        # "covered": opening_hours_weekdays["covered"],
        # "type": opening_hours_weekdays["type"],
        # "usage_minimum": opening_hours_weekdays["usageMinimum"],
        # "price": opening_hours_weekdays["price"],
        self.period_start=self.opening_hours_weekdays["periodStart"],
        self.period_end=self.opening_hours_weekdays["periodEnd"],
        self.grace_periods=self.info_dict["gracePeriods"][0]["minutes"],
        self.weekdays_height_limits=self.info_dict["heightLimits"][0]["height"],
        self.weekdays_facilities=self.info_dict["facilities"],
        self.weekdays_payment_method=self.info_dict["paymentMethods"]

        self.vacancy_type=self.vacancy_dict["vacancy_type"],  # Checkout what does vacancy type = "A" means in the data dict
        self.vacancy=self.vacancy_dict["vacancy"],
        self.last_update=self.vacancy_dict["lastupdate"]

    def get_info_dict(self):

        return {
            "park_id": self.park_id,
            "name": self.name,
            "nature": self.nature,
            "carpark_type": self.carpark_type,
            "floor": self.floor,
            "building_name": self.building_name,
            "street_name": self.street_name,
            "building_no": self.building_no,
            "sub_district": self.sub_district,
            "dc_district": self.dc_district,
            "region": self.region,
            "address": self.address, # Full address
            "district": self.district,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "contact_no": self.contact_no,
            "square": self.square,
            "thumbnail": self.thumbnail,
            "banner": self.banner,
            "website": self.website,
            "opening_status": self.opening_status,
            "weekdays_open": self.weekdays_open,
            "exclude_public_holiday": self.exclude_public_holiday,
            # "remark": opening_hours_weekdays["remark"],
            # "covered": opening_hours_weekdays["covered"],
            # "type": opening_hours_weekdays["type"],
            # "usage_minimum": opening_hours_weekdays["usageMinimum"],
            # "price": opening_hours_weekdays["price"],
            "period_start": self.period_start,
            "period_end": self.period_end,
            "grace_periods": self.grace_periods[0]["minutes"],
            "weekdays_height_limits": self.weekdays_height_limits[0]["height"],
            "weekdays_facilities": self.weekdays_facilities,
            "weekdays_payment_method": self.weekdays_payment_method
        }

    def get_vacancy_dict(self):
        """
        Get the vacancy info of the carpark
        """
        return {
            "vacancy_type": self.vacancy_type,
            "vacancy": self.vacancy,
            "last_update": self.last_update
        }
'''

def main():
    # Get info and vacancy data of all parks with the api url, yielding a list of dictionaries of carparks
    carparks_info, carparks_vacancy = Data(data="info", vehicle_types="privateCar"), Data(data="vacancy", vehicle_types="privateCar")
    # Get the response data
    # carparks_info = data.get_response_data(data="info")["results"] # A list of dictionaries
    # carparks_vacancy = data.get_response_data(data="vacancy")["results"] # A list of dictionaries
    print(f"No. of carparks with vacancy and info: {len(carparks_info.get_response_data())}, {len(carparks_vacancy.get_response_data())}") # 476 carparks
    # Store the info and vacancy of the 1st carpark
    info, vacancy = carparks_info.get_response_data()[0], carparks_vacancy.get_response_data()[0]
    print(f"\nInfo data: {info}")
    print(f"\nVacancy data: {vacancy}")

    # carpark_1 = Carpark(info, vacancy)
    # print(carpark_1.get_info())

    # Extract info from the carpark info dictionary
    address = info.get("address", None)
    rendition_urls = info.get("renditionUrls", None)
    opening_hours_weekdays = None
    if not info.get("openingHours", None) is None:
        opening_hours_weekdays = info["openingHours"][0]

    private_car = info.get("privateCar", None)
    if not private_car.get("hourlyCharges", None) is None:
        for _ in private_car.get("hourlyCharges", None):
            weekdays = [*map(str.lower, _.get("weekdays", None))]
            if weekdays in set("mon", "tue", "wed", "thu", "fri"):
                charges_weekdays = _
            elif weekdays in set("sat", "sun", "ph"):
                charges_weekend = _
            else:
                charges_weekdays, charges_weekend = None, None


    lgv = info.get("LGV", None)
    hgv = info.get("HGV", None)
    coach = info.get("coach", None)
    motor_cycle = info.get("motorCycle", None)

    # Need to differentiate between weekend and weekday
    try:
        info_dict = {
            "park_id": info["park_Id"],
            "name": info["name"],
            "nature": info["nature"],
            "carpark_type": info["carpark_Type"],
            "floor": address["floor"],
            "building_name": address["buildingName"],
            "street_name": address["streetName"],
            "building_no": address["buildingNo"],
            "sub_district": address["subDistrict"],
            "dc_district": address["dcDistrict"],
            "region": address["region"],
            "address": info["displayAddress"], # Full address
            "district": info["district"],
            "latitude": info["latitude"],
            "longitude": info["longitude"],
            "contact_no": info["contactNo"],
            "square": rendition_urls["square"],
            "thumbnail": rendition_urls["thumbnail"],
            "banner": rendition_urls["banner"],
            "website": info["website"],
            "opening_status": info["opening_status"],
            "weekdays_open": opening_hours_weekdays["weekdays"],
            "exclude_public_holiday": opening_hours_weekdays["excludePublicHoliday"],


            "period_start": opening_hours_weekdays["periodStart"],
            "period_end": opening_hours_weekdays["periodEnd"],
            "grace_periods": info["gracePeriods"][0]["minutes"],
            "height_limits": info["heightLimits"][0]["height"],
            "facilities": info["facilities"],
            "payment_method": info["paymentMethods"],


            "private_car_spaceUNL": private_car["spaceUNL"],
            "private_car_spaceEV": private_car["spaceEV"],
            "private_car_spaceDIS": private_car["spaceDIS"],
            "private_car_space": private_car["space"],


            "lgv_spaceUNL": lgv["spaceUNL"],
            "lgv_spaceEV": lgv["spaceEV"],
            "lgv_spaceDIS": lgv["spaceDIS"],
            "lgv_space": lgv["space"],


            "hgv_spaceUNL": hgv["spaceUNL"],
            "hgv_spaceEV": hgv["spaceEV"],
            "hgv_spaceDIS": hgv["spaceDIS"],
            "hgv_space": hgv["space"],


            "coach_spaceUNL": coach["spaceUNL"],
            "coach_spaceEV": coach["spaceEV"],
            "coach_spaceDIS": coach["spaceDIS"],
            "coach_space": coach["space"],


            "motor_cycle_spaceUNL": motor_cycle["spaceUNL"],
            "motor_cycle_spaceEV": motor_cycle["spaceEV"],
            "motor_cycle_spaceDIS": motor_cycle["spaceDIS"],
            "motor_cycle_space": motor_cycle["space"],

            "creation_date": info["creationDate"],
            "modified_date": info["modifiedDate"],
            "published_date": info["publishedDate"],
            "lang": info["lang"],
        }

        private_weekdays_dict = {
            "park_id": info["park_Id"],
            "name": info["name"],
            "private_car_weekdays": charges_weekdays["weekdays"],
            "private_car_exclude_ph": charges_weekdays["excludePublicHoliday"],
            "private_car_remark": charges_weekdays["remark"],
            "private_car_usage_min": charges_weekdays["usageMinimum"],
            "private_car_covered": charges_weekdays["covered"],
            "private_car_type": charges_weekdays["type"],
            "private_car_price": charges_weekdays["price"],
            "private_car_period_start": charges_weekdays["periodStart"],
            "private_car_period_end": charges_weekdays["periodEnd"]
        }
        private_weekend_dict = {
            "park_id": info["park_Id"],
            "name": info["name"],
            "private_car_weekends": charges_weekend["weekdays"],
            "private_car_exclude_ph": charges_weekend["excludePublicHoliday"],
            "private_car_remark": charges_weekend["remark"],
            "private_car_covered": charges_weekend["covered"],
            "private_car_type": charges_weekend["type"],
            "private_car_usage_min": charges_weekend["usageMinimum"],
            "private_car_price": charges_weekend["price"],
            "private_car_period_start": charges_weekend["periodStart"],
            "private_car_period_end": charges_weekend["periodEnd"]
        }

    except KeyError as keyerror1:
        print(f"The key {keyerror1} is not found!")

    # Extract info from the dictionary of carpark vacancy
    private_car_vacancy = vacancy["privateCar"][0]
    try:
        vacancy_dict = {
            "park_id": vacancy["park_Id"],
            "vacancy_type": private_car_vacancy["vacancy_type"], # Checkout what does vacancy type = "A" means in the data dict
            "vacancy": private_car_vacancy["vacancy"],
            "last_update": private_car_vacancy["lastupdate"]
        }
    except KeyError as keyerror2:
        print(f"The key {keyerror2} is not found!")

    df_info = pd.DataFrame([info])
    df_weekdays = pd.DataFrame([private_weekdays_dict])
    df_weekend = pd.DataFrame([private_weekend_dict])
    df_vacancy = pd.DataFrame([vacancy_dict])

    print("End of main()")

if __name__ == "__main__":
    data, vehicle_types, lang = "info", "privateCar", "zh_TW"
    info_url = f"https://api.data.gov.hk/v1/carpark-info-vacancy?data={data}&vehicleTypes={vehicle_types}&lang={lang}"
    data = "vacancy"
    vacancy_url = f"https://api.data.gov.hk/v1/carpark-info-vacancy?data={data}&vehicleTypes={vehicle_types}&lang={lang}"

    info_scraper, vacancy_scraper = Scraper(), Scraper()
    info_scraper.set_url = info_url
    info_scraper.set_decode = "utf-8"

    vacancy_scraper.set_url = vacancy_url
    vacancy_scraper.set_decode = "utf-8"

    try:
        scraped_info = info_scraper.get_dict()["results"]
        print(json.dumps(scraped_info, indent=2))
    except Exception as e:
        print(f"An error occurred: {e}")
        scraped_info = []

    try:
        scraped_vacancy = vacancy_scraper.get_dict()["results"]
        print(json.dumps(scraped_vacancy, indent=2))
    except Exception as e:
        print(f"An error occurred: {e}")
        scraped_vacancy = []

    info_dict, vacancy_dict = scraped_info[0], scraped_vacancy[0]
    print("End of program.")
