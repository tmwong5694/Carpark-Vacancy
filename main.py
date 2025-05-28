from urllib.request import urlopen
import json
import pandas as pd
import sys
from Scraper import *

the_url = "https://api.data.gov.hk/v1/carpark-info-vacancy?data=vacancy&vehicleTypes=privateCar&lang=zh_TW"

class CarparkScraper(Scraper):
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

def get_charges(info_dict, private_car):
    # Get information on the weekdays and weekend charges

    if not private_car.get("hourlyCharges", None) is None:
        # Iterate through the list of hourly_charges
        for hourly_charge in private_car.get("hourlyCharges", None):
            weekdays = {*map(str.lower, hourly_charge.get("weekdays", None))}
            # Store this as weekday charges if any element in weekdays is "mon", "tue", "wed", "thu", "fri"
            if not weekdays.intersection({"mon", "tue", "wed", "thu", "fri"}) == set():
                charges_weekdays = hourly_charge
            # Store this as weekday charges if any element in weekdays is "sat", "sun", "ph"
            elif not weekdays.intersection({"sat", "sun", "ph"}) == set():
                charges_weekend = hourly_charge
            else:
                charges_weekdays, charges_weekend = None, None
    try:
        private_weekdays = {
            "park_id": info_dict["park_Id"],
            "name": info_dict["name"],
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
        private_weekend = {
            "park_id": info_dict["park_Id"],
            "name": info_dict["name"],
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

        private_car_space = {
            "park_id": info_dict["park_Id"],
            "name": info_dict["name"],
            "private_car_spaceUNL": private_car["spaceUNL"],
            "private_car_spaceEV": private_car["spaceEV"],
            "private_car_spaceDIS": private_car["spaceDIS"],
            "private_car_space": private_car["space"]
        }
    except KeyError as keyerror1:
        print(f"The key {keyerror1} is not found!")
    return private_weekdays, private_weekend, private_car_space

def main():
    # Define keywords to input into the scraper
    data, vehicle_types, lang = "info", "privateCar", "zh_TW"
    info_url = f"https://api.data.gov.hk/v1/carpark-info-vacancy?data={data}&vehicleTypes={vehicle_types}&lang={lang}"
    data = "vacancy"
    vacancy_url = f"https://api.data.gov.hk/v1/carpark-info-vacancy?data={data}&vehicleTypes={vehicle_types}&lang={lang}"
    info_scraper, vacancy_scraper = Scraper(url=info_url, decode="utf-8"), Scraper(url=vacancy_url, decode="utf-8")

    try:
        scraped_info = info_scraper.get_dict()["results"]
        print(json.dumps(scraped_info, indent=2))
    except Exception as e:
        print(f"An error occurred: {e}")
        scraped_info = []
    try:
        scraped_vacancy = vacancy_scraper.get_dict()["results"]
        print(json.dumps(scraped_vacancy, indent=2))
        print(f"No. of carparks with vacancy and info: {len(scraped_info)}, {len(scraped_vacancy)}")  # 476 carparks
    except Exception as e:
        print(f"An error occurred: {e}")
        scraped_vacancy = []

    # Store the info and vacancy of the 1st carpark, type: dict
    info_dict, vacancy_dict = scraped_info[0], scraped_vacancy[0]
    print(f"\nInfo data: {info_dict}")
    print(f"\nVacancy data: {vacancy_dict}")


    # Extract sub-dictionary from the carpark info dictionary first
    address = info_dict.get("address", None)
    rendition_urls = info_dict.get("renditionUrls", None)
    opening_hours_weekdays = None
    if not info_dict.get("openingHours", None) is None:
        opening_hours_weekdays = info_dict["openingHours"][0]

    private_car = info_dict.get("privateCar", None)

    lgv = info_dict.get("LGV", None)
    hgv = info_dict.get("HGV", None)
    coach = info_dict.get("coach", None)
    motor_cycle = info_dict.get("motorCycle", None)

    try:
        basic_info = {
            "park_id": info_dict["park_Id"],
            "name": info_dict["name"],
            "nature": info_dict["nature"],
            "carpark_type": info_dict["carpark_Type"],
            "floor": address["floor"],
            "building_name": address["buildingName"],
            "street_name": address["streetName"],
            "building_no": address["buildingNo"],
            "sub_district": address["subDistrict"],
            "dc_district": address["dcDistrict"],
            "region": address["region"],
            "address": info_dict["displayAddress"], # Full address
            "district": info_dict["district"],
            "latitude": info_dict["latitude"],
            "longitude": info_dict["longitude"],
            "contact_no": info_dict["contactNo"],
            "square": rendition_urls["square"],
            "thumbnail": rendition_urls["thumbnail"],
            "banner": rendition_urls["banner"],
            "website": info_dict["website"],
            "opening_status": info_dict["opening_status"],
            "weekdays_open": opening_hours_weekdays["weekdays"],
            "exclude_public_holiday": opening_hours_weekdays["excludePublicHoliday"],


            "period_start": opening_hours_weekdays["periodStart"],
            "period_end": opening_hours_weekdays["periodEnd"],
            "grace_periods": info_dict["gracePeriods"][0]["minutes"],
            "height_limits": info_dict["heightLimits"][0]["height"],
            "facilities": info_dict["facilities"],
            "payment_method": info_dict["paymentMethods"],








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

            "creation_date": info_dict["creationDate"],
            "modified_date": info_dict["modifiedDate"],
            "published_date": info_dict["publishedDate"],
            "lang": info_dict["lang"],
        }

    except KeyError as keyerror1:
        print(f"The key {keyerror1} is not found!")

    # Extract info from the dictionary of carpark vacancy
    private_car_vacancy = vacancy_dict["privateCar"][0]
    try:
        vacancy= {
            "park_id": vacancy_dict["park_Id"],
            "name": info_dict["name"],
            "vacancy_type": private_car_vacancy["vacancy_type"], # Checkout what does vacancy type = "A" means in the data dict
            "vacancy": private_car_vacancy["vacancy"],
            "last_update": private_car_vacancy["lastupdate"]
        }
    except KeyError as keyerror2:
        print(f"The key {keyerror2} is not found!")

    df_info = pd.DataFrame([info_dict])
    df_weekdays = pd.DataFrame([get_charges(info_dict=info_dict, private_car=private_car)[0]])
    df_weekend = pd.DataFrame([get_charges(info_dict=info_dict, private_car=private_car)[1]])
    df_private = pd.DataFrame([get_charges(info_dict=info_dict, private_car=private_car)[2]])
    df_lgv = pd.DataFrame({
        "lgv_spaceUNL": lgv["spaceUNL"],
        "lgv_spaceEV": lgv["spaceEV"],
        "lgv_spaceDIS": lgv["spaceDIS"],
        "lgv_space": lgv["space"]
    }, index=[0])

    df_vacancy = pd.DataFrame([vacancy])

    print("End of main()")

if __name__ == "__main__":
    main()
    print("End of program.")
