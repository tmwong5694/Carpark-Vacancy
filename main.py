import bs4
from urllib.request import urlopen
import json

api_url = "https://api.data.gov.hk/v1/carpark-info-vacancy?data=vacancy&vehicleTypes=privateCar&lang=en_US"

def get_response_data(url):
    try:
        # https://api.data.gov.hk/v1/carpark-info-vacancy?data=<param>&vehicleTypes=<param>&carparkIds=<param>&extent=<param>&lang=<param>
        # data = "info" or "vacancy"
        # vehicleTypes = "privateCar", "LGV", "HGV", "CV", "coach", "motorCycle"
        # lang = "en_US", "zh_HK", "zh_CN"
        # Get response from the Carpark Vacancy API
        with urlopen(url) as response:
            response_data = response.read().decode("utf-8")
            response_data = json.loads(response_data)  # Parse the response data

        print("Response code:", response.getcode())
    except:
        print("Error: Unable to connect to the API.")
        exit(1)
    return response_data

def main():
    vacancy_data = get_response_data(api_url)["result"]
    print(f"Response data: {vacancy_data}")


if __name__ == '__main__':
    main()
