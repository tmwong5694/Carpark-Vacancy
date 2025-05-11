from urllib.request import urlopen
import json
import pandas as pd

the_url = "https://api.data.gov.hk/v1/carpark-info-vacancy?data=vacancy&vehicleTypes=privateCar&lang=en_US"

# def get_response_data(input_url):
def get_response_data(data_input: str = "info", vehicleTypes_input: str = "privateCar", carparkIds_input = None, extent_input = None, lang_input: str = "zh_TW"):

    # https://api.data.gov.hk/v1/carpark-info-vacancy?data=<param>&vehicleTypes=<param>&carparkIds=<param>&extent=<param>&lang=<param>
    # data = "info" or "vacancy"
    # vehicleTypes = "privateCar", "LGV", "HGV", "CV", "coach", "motorCycle"
    # lang = "en_US", "zh_HK", "zh_CN"
    # Get response from the Carpark Vacancy API
    if carparkIds_input is None and extent_input is None:
        input_url = f"https://api.data.gov.hk/v1/carpark-info-vacancy?data={data_input}&vehicleTypes={vehicleTypes_input}&lang={lang_input}"
    elif not carparkIds_input is None and extent_input is None:
        input_url = f"https://api.data.gov.hk/v1/carpark-info-vacancy?data={data_input}&vehicleTypes={vehicleTypes_input}&carparkIds={carparkIds_input}&lang={lang_input}"
    elif carparkIds_input is None and not extent_input is None:
        input_url = f"https://api.data.gov.hk/v1/carpark-info-vacancy?data={data_input}&vehicleTypes={vehicleTypes_input}&extent={extent_input}&lang={lang_input}" 
    try:
        with urlopen(input_url) as response:
            response_data = response.read().decode("utf-8")
            response_data = json.loads(response_data)  # Parse the response data

        print("Response code:", response.getcode())
    except:
        print("Error: Unable to connect to the API.")
        exit(1)
    return response_data

def main():
    # Get info and vacancy data of all parks with the api url
    carparks_info = get_response_data(data_input="info", vehicleTypes_input="privateCar", lang_input="zh_TW")["results"] # A list of dictionaries
    carparks_vacancy = get_response_data(data_input="vacancy", vehicleTypes_input="privateCar", lang_input="zh_TW")["results"] # A list of dictionaries
    print(f"No. of carparks with vacancy and info: {len(carparks_vacancy)}, {len(carparks_info)}") # 476

    # Store the info and vacancy of the 1st carpark
    info_dict, vacancy_dict = carparks_info[0], carparks_vacancy[0]
    
    print(f"\nInfo data: {info_dict}")
    print(f"\nVacancy data: {vacancy_dict}")

    # Extract info from the carpark info dictionary
    address = info_dict["address"]
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
            "rendition_urls": info_dict["renditionUrls"]

        }
    except KeyError as keyerror1:
        print(f"The key {keyerror1} is not found!")
    
    # Turn the list into a dataframe
    carpark_info_list = [carpark_info]
    info_df = pd.DataFrame(carpark_info_list, index=[0])
    # Extract info from the dictionary of carpark vacancy
    private_car = vacancy_dict["privateCar"][0]
    try:
        carpark_vacancy = {
            "park_id": vacancy_dict["park_Id"],
            "vacancy_type": private_car["vacancy_type"], # Checkout what does vacancy type = "A" means in the data dict
            "vacancy": private_car["vacancy"],
            "last_update": private_car["lastupdate"]
        }
    except KeyError as keyerror2:
        print(f"The key {keyerror2} is not found!")
    
    carpark_vacancy_list = [carpark_vacancy]
    vacancy_df = pd.DataFrame(carpark_vacancy_list, index=[0])
    print(info_df, vacancy_df, sep="\n")


if __name__ == "__main__":
    main()
