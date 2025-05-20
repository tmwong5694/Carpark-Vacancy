from urllib.request import urlopen
import json
import pandas as pd
from torch.nn.utils import skip_init

the_url = "https://api.data.gov.hk/v1/carpark-info-vacancy?data=vacancy&vehicleTypes=privateCar&lang=en_US"

class Data():
    def __init__(self, data: str="vehicle", vehicleTypes: str="privateCar", lang: str="zh_TW", carparkIds=None, extent=None):
        self.data = data
        self.vehicleTypes = vehicleTypes
        self.lang = lang
        self.carparkIds = carparkIds
        self.extent = extent

    def get_response_data(self):
        """
        https://api.data.gov.hk/v1/carpark-info-vacancy?data=<param>&vehicleTypes=<param>&carparkIds=<param>&extent=<param>&lang=<param>
        data = "info" or "vacancy"
        vehicleTypes = "privateCar", "LGV", "HGV", "CV", "coach", "motorCycle"
        lang = "en_US", "zh_HK", "zh_CN"
        """
        # Get response from the Carpark Vacancy API
        if self.carparkIds is None and self.extent is None:
            input_url = f"https://api.data.gov.hk/v1/carpark-info-vacancy?data={self.data}&vehicleTypes={self.vehicleTypes}&lang={self.lang}"
        elif not self.carparkIds is None and self.extent is None:
            input_url = f"https://api.data.gov.hk/v1/carpark-info-vacancy?data={self.data}&vehicleTypes={self.vehicleTypes}&carparkIds={self.carparkIds}&lang={self.lang}"
        elif self.carparkIds is None and not self.extent is None:
            input_url = f"https://api.data.gov.hk/v1/carpark-info-vacancy?data={self.data}&vehicleTypes={self.vehicleTypes}&extent={self.extent}&lang={self.lang}"
        try:
            with urlopen(input_url) as response:
                response_data = response.read().decode("utf-8")
                response_data = json.loads(response_data)  # Parse the response data
            print("Response code:", response.getcode())
        except:
            print("Error: Unable to connect to the API.")
            exit(1)
        return response_data

# class Carpark():
#     def __init__(self):


def main():
    ## Get info and vacancy data of all parks with the api url
    data = Data(data="vehicle", vehicleTypes="privateCar", lang="zh_TW", carparkIds=None, extent=None)
    # Get the response data
    carparks_vacancy = data.get_response_data()["results"] # A list of dictionaries
    carparks_info = data.get_response_data()["results"] # A list of dictionaries
    print(f"No. of carparks with vacancy and info: {len(carparks_vacancy)}, {len(carparks_info)}") # 476 carparks
    # Store the info and vacancy of the 1st carpark
    info_dict, vacancy_dict = carparks_info[0], carparks_vacancy[0]
    print(f"\nInfo data: {info_dict}")
    print(f"\nVacancy data: {vacancy_dict}")
    # Extract info from the carpark info dictionary
    address = info_dict["address"]
    rendition_urls = info_dict["renditionUrls"]
    opening_hours_weekdays = info_dict["openingHours"][0]
    # Need to differentiate between weekend and weekday
    try:
        carpark_info = {
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
            # "remark": opening_hours_weekdays["remark"],
            # "covered": opening_hours_weekdays["covered"],
            # "type": opening_hours_weekdays["type"],
            # "usage_minimum": opening_hours_weekdays["usageMinimum"],
            # "price": opening_hours_weekdays["price"],
            "period_start": opening_hours_weekdays["periodStart"],
            "period_end": opening_hours_weekdays["periodEnd"],
            "grace_periods": info_dict["gracePeriods"][0]["minutes"],
            "weekdays_height_limits": info_dict["heightLimits"][0]["height"],
            "weekdays_facilities": info_dict["facilities"],
            "weekdays_payment_method": info_dict["paymentMethods"]

        }
    except KeyError as keyerror1:
        print(f"The key {keyerror1} is not found!")
    
    # Turn the list into a dataframe
    carpark_info_list = [carpark_info]

    info_df = pd.DataFrame(carpark_info_list, index=[0])
    # Extract info from the dictionary of carpark vacancy
    private_car_vacancy = vacancy_dict["privateCar"][0]
    try:
        carpark_vacancy = {
            "park_id": vacancy_dict["park_Id"],
            "vacancy_type": private_car_vacancy["vacancy_type"], # Checkout what does vacancy type = "A" means in the data dict
            "vacancy": private_car_vacancy["vacancy"],
            "last_update": private_car_vacancy["lastupdate"]
        }
    except KeyError as keyerror2:
        print(f"The key {keyerror2} is not found!")
    
    carpark_vacancy_list = [carpark_vacancy]
    vacancy_df = pd.DataFrame(carpark_vacancy_list, index=[0])
    # print(info_df, vacancy_df, sep="\n") # print


if __name__ == "__main__":
    main()
