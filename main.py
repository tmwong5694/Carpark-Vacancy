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
    def __init__(self):
        pass

    def get_response_data(self, data: str="info", vehicle_types: str="privateCar", lang: str="zh_TW", carpark_ids=None, extent=None, ) -> dict:
        """
        https://api.data.gov.hk/v1/carpark-info-vacancy?data=<param>&vehicleTypes=<param>&carparkIds=<param>&extent=<param>&lang=<param>
        data = "info" or "vacancy"
        vehicleTypes = "privateCar", "LGV", "HGV", "CV", "coach", "motorCycle"
        lang = "en_US", "zh_TW", "zh_CN"
        """

        def check_input() -> None:
            """Validate the input of params"""
            input_choice = {
                "data": ["info", "vacancy"],
                "vehicleTypes": ["privateCar", "LGV", "HGV", "CV", "coach", "motorCycle"],
                "lang": ["en_US", "zh_TW", "zh_CN"]
            }
            if not data in input_choice["data"]:
                raise ValueError(f"Input should be one of {input_choice['data']}. Got {data}.")
            if not vehicle_types in input_choice["vehicleTypes"]:
                raise ValueError(f"Input should be one of {input_choice['vehicleTypes']}. Got {data}.")
            if not lang in input_choice["lang"]:
                raise ValueError(f"Input should be one of {input_choice['lang']}. Got {data}.")

        check_input()

        # Get response from the Carpark Vacancy API
        if carpark_ids is None and extent is None:
            ####
            input_url = f"https://api.data.gov.hk/v1/carpark-info-vacancy?data={data}&vehicleTypes={vehicle_types}&lang={lang}"
        elif not carpark_ids is None and extent is None:
            input_url = f"https://api.data.gov.hk/v1/carpark-info-vacancy?data={data}&vehicleTypes={vehicle_types}&carparkIds={carpark_ids}&lang={lang}"
        elif carpark_ids is None and not extent is None:
            input_url = f"https://api.data.gov.hk/v1/carpark-info-vacancy?data={data}&vehicleTypes={vehicle_types}&extent={extent}&lang={lang}"
        try:
            with urlopen(input_url) as response:
                response_data = response.read().decode("utf-8")
                response_data = json.loads(response_data)  # Parse the response data
            print("Response code:", response.getcode())
        except urllib.error.HTTPError as HTTPError:
            print(f"Error: Unable to connect to the API:\n{HTTPError}")
            exit(1)
        return response_data["results"]

def get_charges(info_dict):
    # Get information on the weekdays and weekend charges
    private_car = info_dict.get("privateCar")
    if not private_car.get("hourlyCharges") is None:
        # Iterate through the list of hourly_charges
        for hourly_charge in private_car.get("hourlyCharges"):
            weekdays = {*map(str.lower, hourly_charge.get("weekdays"))}
            # Store this as weekday charges if any element in weekdays contains "mon", "tue", "wed", "thu", "fri"
            if len(weekdays.intersection({"mon", "tue", "wed", "thu", "fri"})) > 0:
                charges_weekdays = hourly_charge
            # Store this as weekday charges if any element in weekdays contains "sat", "sun", "ph"
            elif len(weekdays.intersection({"sat", "sun", "ph"})) > 0:
                charges_weekend = hourly_charge
            else:
                charges_weekdays, charges_weekend = None
    try:
        private_weekdays = {
            "park_id": info_dict.get("park_Id"),
            "name": info_dict.get("name"),
            "private_car_weekdays": charges_weekdays.get("weekdays"),
            "private_car_exclude_ph": charges_weekdays.get("excludePublicHoliday"),
            "private_car_remark": charges_weekdays.get("remark"),
            "private_car_usage_min": charges_weekdays.get("usageMinimum"),
            "private_car_covered": charges_weekdays.get("covered"),
            "private_car_type": charges_weekdays.get("type"),
            "private_car_price": charges_weekdays.get("price"),
            "private_car_period_start": charges_weekdays.get("periodStart"),
            "private_car_period_end": charges_weekdays.get("periodEnd")
        }
        private_weekend = {
            "park_id": info_dict.get("park_Id"),
            "name": info_dict.get("name"),
            "private_car_weekends": charges_weekend.get("weekdays"),
            "private_car_exclude_ph": charges_weekend.get("excludePublicHoliday"),
            "private_car_remark": charges_weekend.get("remark"),
            "private_car_covered": charges_weekend.get("covered"),
            "private_car_type": charges_weekend.get("type"),
            "private_car_usage_min": charges_weekend.get("usageMinimum"),
            "private_car_price": charges_weekend.get("price"),
            "private_car_period_start": charges_weekend.get("periodStart"),
            "private_car_period_end": charges_weekend.get("periodEnd")
        }
    except KeyError as keyerror1:
        print(f"The key {keyerror1} is not found!")
    return private_weekdays, private_weekend

def get_carpark_json(info_dict, vacancy_dict):
    """"""

    # Extract sub-dictionary from the carpark info dictionary first
    address = info_dict.get("address")
    rendition_urls = info_dict.get("renditionUrls")
    opening_hours_weekdays = None
    if not info_dict.get("openingHours") is None:
        opening_hours_weekdays = info_dict["openingHours"][0]

    try:
        info_json = {
            "park_id": info_dict.get("park_Id"),
            "name": info_dict.get("name"),
            "nature": info_dict.get("nature"),
            "carpark_type": info_dict.get("carpark_Type"),
            "floor": address.get("floor"),
            "building_name": address.get("buildingName"),
            "street_name": address.get("streetName"),
            "building_no": address.get("buildingNo"),
            "sub_district": address.get("subDistrict"),
            "dc_district": address.get("dcDistrict"),
            "region": address.get("region"),
            "address": info_dict.get("displayAddress"),  # Full address
            "district": info_dict.get("district"),
            "latitude": info_dict.get("latitude"),
            "longitude": info_dict.get("longitude"),
            "contact_no": info_dict.get("contactNo"),
            "square": rendition_urls.get("square"),
            "thumbnail": rendition_urls.get("thumbnail"),
            "banner": rendition_urls.get("banner"),
            "website": info_dict.get("website"),
            "opening_status": info_dict.get("opening_status"),
            "weekdays_open": opening_hours_weekdays.get("weekdays"),
            "exclude_public_holiday": opening_hours_weekdays.get("excludePublicHoliday"),

            "period_start": opening_hours_weekdays.get("periodStart"),
            "period_end": opening_hours_weekdays.get("periodEnd"),
            "grace_periods": info_dict.get("gracePeriods")[0]["minutes"],
            "height_limits": info_dict.get("heightLimits")[0]["height"],
            "facilities": info_dict.get("facilities"),
            "payment_method": info_dict.get("paymentMethods"),

            "creation_date": info_dict.get("creationDate"),
            "modified_date": info_dict.get("modifiedDate"),
            "published_date": info_dict.get("publishedDate"),
            "lang": info_dict.get("lang"),
        }

    except KeyError as keyerror1:
        print(f"The key {keyerror1} is not found!")

    # Extract info from the dictionary of carpark vacancy
    private_car_vacancy = vacancy_dict.get("privateCar")[0]
    try:
        vacancy_json = {
            "park_id": vacancy_dict.get("park_Id"),
            "name": info_dict.get("name"),
            "vacancy_type": private_car_vacancy.get("vacancy_type"),
            # Checkout what does vacancy type = "A" means in the data dict
            "vacancy": private_car_vacancy.get("vacancy"),
            "last_update": private_car_vacancy.get("lastupdate")
        }
    except KeyError as keyerror2:
        print(f"The key {keyerror2} is not found!")

    info_df = pd.DataFrame([info_json])
    weekdays_df = pd.DataFrame([get_charges(info_dict=info_dict)[0]])
    weekend_df = pd.DataFrame([get_charges(info_dict=info_dict)[1]])

    private_car = info_dict.get("privateCar")
    private_car_df = pd.DataFrame([{
        "park_id": info_dict.get("park_Id"),
        "name": info_dict.get("name"),
        "private_car_spaceUNL": private_car.get("spaceUNL"),
        "private_car_spaceEV": private_car.get("spaceEV"),
        "private_car_spaceDIS": private_car.get("spaceDIS"),
        "private_car_space": private_car.get("space")
    }])
    lgv = info_dict.get("LGV")
    lgv_df = pd.DataFrame([{
        "park_id": info_dict.get("park_Id"),
        "name": info_dict.get("name"),
        "lgv_spaceUNL": lgv.get("spaceUNL"),
        "lgv_spaceEV": lgv.get("spaceEV"),
        "lgv_spaceDIS": lgv.get("spaceDIS"),
        "lgv_space": lgv.get("space")
    }])
    hgv = info_dict.get("HGV")
    hgv_df = pd.DataFrame([{
        "park_id": info_dict.get("park_Id"),
        "name": info_dict.get("name"),
        "hgv_spaceUNL": hgv.get("spaceUNL"),
        "hgv_spaceEV": hgv.get("spaceEV"),
        "hgv_spaceDIS": hgv.get("spaceDIS"),
        "hgv_space": hgv.get("space")
    }])
    coach = info_dict.get("coach")
    coach_df = pd.DataFrame([{
        "park_id": info_dict.get("park_Id"),
        "name": info_dict.get("name"),
        "coach_spaceUNL": coach.get("spaceUNL"),
        "coach_spaceEV": coach.get("spaceEV"),
        "coach_spaceDIS": coach.get("spaceDIS"),
        "coach_space": coach.get("space")
    }])
    motor_cycle = info_dict.get("motorCycle")
    motorcycle_df = pd.DataFrame([{
        "park_id": info_dict.get("park_Id"),
        "name": info_dict.get("name"),
        "motor_cycle_spaceUNL": motor_cycle.get("spaceUNL"),
        "motor_cycle_spaceEV": motor_cycle.get("spaceEV"),
        "motor_cycle_spaceDIS": motor_cycle.get("spaceDIS"),
        "motor_cycle_space": motor_cycle.get("space")
    }])
    vacancy_df = pd.DataFrame([vacancy_json])
    carpark_dict = {
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
    return carpark_dict

def get_public_holiday():
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

    tables = get_carpark_json(info_dict=info_dict, vacancy_dict=vacancy_dict)

    holiday_url = "https://www.gov.hk/en/about/abouthk/holiday/2025.htm"
    html_content = Scraper(url=holiday_url, decode="utf-8").openurl()
    soup = BeautifulSoup(html_content, features="html.parser")

    scraped_list = soup.find_all("table")


    print("End of main()")

if __name__ == "__main__":
    # main()

    scraper = CarparkScraper()
    data, vacancy = scraper.get_response_data(), scraper.get_response_data(data="vacancy")
    ph_table = get_public_holiday()
    # print(ph_table)


    print("End of program.")
