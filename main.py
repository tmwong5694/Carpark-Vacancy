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

def get_charges(info_dict):
    # Get information on the weekdays and weekend charges
    private_car = info_dict.get("privateCar", None)
    if not private_car.get("hourlyCharges", None) is None:
        # Iterate through the list of hourly_charges
        for hourly_charge in private_car.get("hourlyCharges", None):
            weekdays = {*map(str.lower, hourly_charge.get("weekdays", None))}
            # Store this as weekday charges if any element in weekdays contains "mon", "tue", "wed", "thu", "fri"
            if len(weekdays.intersection({"mon", "tue", "wed", "thu", "fri"})) > 0:
                charges_weekdays = hourly_charge
            # Store this as weekday charges if any element in weekdays contains "sat", "sun", "ph"
            elif len(weekdays.intersection({"sat", "sun", "ph"})) > 0:
                charges_weekend = hourly_charge
            else:
                charges_weekdays, charges_weekend = None, None
    try:
        private_weekdays = {
            "park_id": info_dict.get("park_Id", None),
            "name": info_dict.get("name", None),
            "private_car_weekdays": charges_weekdays.get("weekdays", None),
            "private_car_exclude_ph": charges_weekdays.get("excludePublicHoliday", None),
            "private_car_remark": charges_weekdays.get("remark", None),
            "private_car_usage_min": charges_weekdays.get("usageMinimum", None),
            "private_car_covered": charges_weekdays.get("covered", None),
            "private_car_type": charges_weekdays.get("type", None),
            "private_car_price": charges_weekdays.get("price", None),
            "private_car_period_start": charges_weekdays.get("periodStart", None),
            "private_car_period_end": charges_weekdays.get("periodEnd", None)
        }
        private_weekend = {
            "park_id": info_dict.get("park_Id", None),
            "name": info_dict.get("name", None),
            "private_car_weekends": charges_weekend.get("weekdays", None),
            "private_car_exclude_ph": charges_weekend.get("excludePublicHoliday", None),
            "private_car_remark": charges_weekend.get("remark", None),
            "private_car_covered": charges_weekend.get("covered", None),
            "private_car_type": charges_weekend.get("type", None),
            "private_car_usage_min": charges_weekend.get("usageMinimum", None),
            "private_car_price": charges_weekend.get("price", None),
            "private_car_period_start": charges_weekend.get("periodStart", None),
            "private_car_period_end": charges_weekend.get("periodEnd", None)
        }
    except KeyError as keyerror1:
        print(f"The key {keyerror1} is not found!")
    return private_weekdays, private_weekend

def get_tables(info_dict, vacancy_dict):
    """"""

    # Extract sub-dictionary from the carpark info dictionary first
    address = info_dict.get("address", None)
    rendition_urls = info_dict.get("renditionUrls", None)
    opening_hours_weekdays = None
    if not info_dict.get("openingHours", None) is None:
        opening_hours_weekdays = info_dict["openingHours"][0]

    try:
        info = {
            "park_id": info_dict.get("park_Id", None),
            "name": info_dict.get("name", None),
            "nature": info_dict.get("nature", None),
            "carpark_type": info_dict.get("carpark_Type", None),
            "floor": address.get("floor", None),
            "building_name": address.get("buildingName", None),
            "street_name": address.get("streetName", None),
            "building_no": address.get("buildingNo", None),
            "sub_district": address.get("subDistrict", None),
            "dc_district": address.get("dcDistrict", None),
            "region": address.get("region", None),
            "address": info_dict.get("displayAddress", None),  # Full address
            "district": info_dict.get("district", None),
            "latitude": info_dict.get("latitude", None),
            "longitude": info_dict.get("longitude", None),
            "contact_no": info_dict.get("contactNo", None),
            "square": rendition_urls.get("square", None),
            "thumbnail": rendition_urls.get("thumbnail", None),
            "banner": rendition_urls.get("banner", None),
            "website": info_dict.get("website", None),
            "opening_status": info_dict.get("opening_status", None),
            "weekdays_open": opening_hours_weekdays.get("weekdays", None),
            "exclude_public_holiday": opening_hours_weekdays.get("excludePublicHoliday", None),

            "period_start": opening_hours_weekdays.get("periodStart", None),
            "period_end": opening_hours_weekdays.get("periodEnd", None),
            "grace_periods": info_dict.get("gracePeriods", None)[0]["minutes"],
            "height_limits": info_dict.get("heightLimits", None)[0]["height"],
            "facilities": info_dict.get("facilities", None),
            "payment_method": info_dict.get("paymentMethods", None),

            "creation_date": info_dict.get("creationDate", None),
            "modified_date": info_dict.get("modifiedDate", None),
            "published_date": info_dict.get("publishedDate", None),
            "lang": info_dict.get("lang", None),
        }

    except KeyError as keyerror1:
        print(f"The key {keyerror1} is not found!")

    # Extract info from the dictionary of carpark vacancy
    private_car_vacancy = vacancy_dict.get("privateCar", None)[0]
    try:
        vacancy = {
            "park_id": vacancy_dict.get("park_Id", None),
            "name": info_dict.get("name", None),
            "vacancy_type": private_car_vacancy.get("vacancy_type", None),
            # Checkout what does vacancy type = "A" means in the data dict
            "vacancy": private_car_vacancy.get("vacancy", None),
            "last_update": private_car_vacancy.get("lastupdate", None)
        }
    except KeyError as keyerror2:
        print(f"The key {keyerror2} is not found!")

    info_df = pd.DataFrame([info])
    weekdays_df = pd.DataFrame([get_charges(info_dict=info_dict)[0]])
    weekend_df = pd.DataFrame([get_charges(info_dict=info_dict)[1]])
    private_car = info_dict.get("privateCar", None)
    private_car_df = pd.DataFrame({
        "park_id": info_dict.get("park_Id", None),
        "name": info_dict.get("name", None),
        "private_car_spaceUNL": private_car.get("spaceUNL", None),
        "private_car_spaceEV": private_car.get("spaceEV", None),
        "private_car_spaceDIS": private_car.get("spaceDIS", None),
        "private_car_space": private_car.get("space", None)
    }, index=[0])
    lgv = info_dict.get("LGV", None)
    lgv_df = pd.DataFrame({
        "park_id": info_dict.get("park_Id", None),
        "name": info_dict.get("name", None),
        "lgv_spaceUNL": lgv.get("spaceUNL", None),
        "lgv_spaceEV": lgv.get("spaceEV", None),
        "lgv_spaceDIS": lgv.get("spaceDIS", None),
        "lgv_space": lgv.get("space", None)
    }, index=[0])
    hgv = info_dict.get("HGV", None)
    hgv_df = pd.DataFrame({
        "park_id": info_dict.get("park_Id", None),
        "name": info_dict.get("name", None),
        "hgv_spaceUNL": hgv.get("spaceUNL", None),
        "hgv_spaceEV": hgv.get("spaceEV", None),
        "hgv_spaceDIS": hgv.get("spaceDIS", None),
        "hgv_space": hgv.get("space", None)
    }, index=[0])
    coach = info_dict.get("coach", None)
    coach_df = pd.DataFrame({
        "park_id": info_dict.get("park_Id", None),
        "name": info_dict.get("name", None),
        "coach_spaceUNL": coach.get("spaceUNL", None),
        "coach_spaceEV": coach.get("spaceEV", None),
        "coach_spaceDIS": coach.get("spaceDIS", None),
        "coach_space": coach.get("space", None)
    }, index=[0])
    motor_cycle = info_dict.get("motorCycle", None)
    motorcycle_df = pd.DataFrame({
        "park_id": info_dict.get("park_Id", None),
        "name": info_dict.get("name", None),
        "motor_cycle_spaceUNL": motor_cycle.get("spaceUNL", None),
        "motor_cycle_spaceEV": motor_cycle.get("spaceEV", None),
        "motor_cycle_spaceDIS": motor_cycle.get("spaceDIS", None),
        "motor_cycle_space": motor_cycle.get("space", None)
    }, index=[0])
    vacancy_df = pd.DataFrame(vacancy, index=[0])
    tables_dict = {
        "info": info_df,
        "weekdays": weekdays_df,
        "weekend": weekend_df,
        "private_car": private_car_df,
        "lgv": lgv_df,
        "hgv": hgv_df,
        "coach": coach_df,
        "motorcycle": motorcycle_df,
        "vacancy": vacancy_df
    }
    return tables_dict

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
        print(f"An error occurred when calling Scraper.get_dict(): {e}")
        scraped_info = []
    try:
        scraped_vacancy = vacancy_scraper.get_dict()["results"]
        print(json.dumps(scraped_vacancy, indent=2))
        print(f"No. of carparks with vacancy and info: {len(scraped_info)}, {len(scraped_vacancy)}")  # 476 carparks
    except Exception as e:
        print(f"An error occurred when calling Scraper.get_dict(): {e}")

    # Store the info and vacancy of the 1st carpark, type: dict
    info_dict, vacancy_dict = scraped_info[0], scraped_vacancy[0]
    print(f"\nInfo data: {info_dict}\nVacancy data: {vacancy_dict}")

    tables = get_tables(info_dict=info_dict, vacancy_dict=vacancy_dict)

    scraped_text = Scraper().openurl()


    print("End of main()")

if __name__ == "__main__":
    main()
    print("End of program.")
