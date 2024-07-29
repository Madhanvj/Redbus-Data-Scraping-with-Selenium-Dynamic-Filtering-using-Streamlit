from selenium import webdriver
from selenium.common.exceptions import ElementClickInterceptedException, NoSuchElementException
from selenium.webdriver.common.by import By
import time
from datetime import datetime
import pandas as pd

# Initialize the WebDriver
def initialize_driver():
    driver = webdriver.Chrome()
    driver.maximize_window()
    return driver

# Function to scroll to end of page to get all buses
def scroll(driver):
    last_height = driver.execute_script("return document.body.scrollHeight")

    while True:
        # Scroll down to the bottom
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        # Wait for page to load
        time.sleep(0.2)
        # Calculate new scroll height and compare with last scroll height
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

# Function to get bus details
def get_bus_details_for_route(driver, url):
    driver.get(url)
    route_info = (driver.find_element(By.CLASS_NAME, 'D136_h1').text).replace(' Bus', '')
    dayelem = driver.find_element(By.XPATH, '//*[@id="searchDat"]')
    dayvar = (dayelem.get_attribute('value')) + " 2024"

    # Waiting for page to load
    time.sleep(4)

    # Expanding all the buses section if it has 'view buses' button
    try:
        tourismBusesAgency = driver.find_elements(By.CLASS_NAME, 'gmeta-data.clearfix')
        for agency in tourismBusesAgency:
            btn_var = agency.find_element(By.CLASS_NAME, "button")
            btn_var.click()
    except ElementClickInterceptedException:
        print(url, 'view-buses in this url not clickable')

    # Scrolling so all the bus data gets loaded
    scroll(driver)

    # Creating empty list to store the details of the buses
    buses_list = []

    # Getting all the buses group so that we can get all the bus details for each of them i.e. states and private buses
    busesgroup = driver.find_elements(By.CLASS_NAME, 'bus-items')

    for buses in busesgroup:
        buseslist = buses.find_elements(By.CLASS_NAME, 'clearfix.bus-item')

        # Gets all the required bus details in the selected group
        for bus in buseslist:
            busname = bus.find_element(By.CLASS_NAME, 'travels.lh-24.f-bold.d-color').text
            bustype = bus.find_element(By.CLASS_NAME, 'bus-type.f-12.m-top-16.l-color.evBus').text
            departing_time = dayvar + " " + bus.find_element(By.CLASS_NAME, 'dp-time.f-19.d-color.f-bold').text + ":00"
            duration = bus.find_element(By.CLASS_NAME, 'dur.l-color.lh-24').text

            # Some buses have reaching date as the next day, so we are using try and exception to get these values
            try:
                bus.find_element(By.CLASS_NAME, 'next-day-dp-lbl.m-top-16') != 0
                new_date = ((bus.find_element(By.CLASS_NAME, 'next-day-dp-lbl.m-top-16').text).replace("-", " ")) + " 2024"
                reaching_time = new_date + " " + bus.find_element(By.CLASS_NAME, 'bp-time.f-19.d-color.disp-Inline').text + ":00"
            except NoSuchElementException:
                reaching_time = dayvar + " " + bus.find_element(By.CLASS_NAME, 'bp-time.f-19.d-color.disp-Inline').text + ":00"

            star_rating = (bus.find_element(By.CLASS_NAME, 'column-six.p-right-10.w-10.fl').text)[0:3]
            price = (bus.find_element(By.CLASS_NAME, 'fare.d-block').text).replace("INR ", "")
            seats_available = (bus.find_element(By.CLASS_NAME, 'column-eight.w-15.fl').text)[0:2]

            bus_item = {
                'route_name': route_info,
                'route_link': url,
                'busname': busname,
                'bustype': bustype,
                'departing_time': datetime.strptime(departing_time, "%d %b %Y %H:%M:%S"),
                'duration': duration,
                'reaching_time': datetime.strptime(reaching_time, "%d %b %Y %H:%M:%S"),
                'star_rating': float(star_rating),
                'price': float(price),
                'seats_available': int(seats_available[0:2])
            }

            buses_list.append(bus_item)

    df = pd.DataFrame(buses_list)
    return df

# Function to get all the routes URL for the selected state transportation
def get_urls(driver, url):
    driver.get(url)
    time.sleep(2)

    routes_list_to_get_urls = driver.find_elements(By.CLASS_NAME, 'route_link')
    routes_urls = []

    for routes in routes_list_to_get_urls:
        url_to_extract = routes.find_element(By.TAG_NAME, 'a')
        url_extracted = url_to_extract.get_attribute('href')
        routes_urls.append(url_extracted)

    # To handle pagination
    page_elements = driver.find_elements(By.CLASS_NAME, 'DC_117_pageTabs')

    for page in page_elements:
        try:
            page.click()
            print(page.text)
            time.sleep(0.2)
            routes_list_to_get_urls = driver.find_elements(By.CLASS_NAME, 'route_link')
            for routes in routes_list_to_get_urls:
                url_to_extract = routes.find_element(By.TAG_NAME, 'a')
                url_extracted = url_to_extract.get_attribute('href')
                routes_urls.append(url_extracted)
        except ElementClickInterceptedException:
            print('The page is not clickable')

    return routes_urls

list_of_state_tourism_url = [
    'https://www.redbus.in/online-booking/ksrtc-kerala/?utm_source=rtchometile',
    'https://www.redbus.in/online-booking/ktcl/?utm_source=rtchometile',
    'https://www.redbus.in/online-booking/wbtc-ctc/?utm_source=rtchometile',
    'https://www.redbus.in/online-booking/apsrtc/?utm_source=rtchometile',
    'https://www.redbus.in/online-booking/astc/?utm_source=rtchometile',
    'https://www.redbus.in/online-booking/pepsu/?utm_source=rtchometile',
    'https://www.redbus.in/online-booking/jksrtc',
    'https://www.redbus.in/online-booking/tsrtc/?utm_source=rtchometile',
    'https://www.redbus.in/online-booking/rsrtc/?utm_source=rtchometile',
    'https://www.redbus.in/online-booking/hrtc/?utm_source=rtchometile'
]

all_routes_urls = []

# Collecting all the URLs for routes from the state tourism URLs
for url in list_of_state_tourism_url:
    driver = initialize_driver()
    routes_urls = get_urls(driver, url)
    all_routes_urls.extend(routes_urls)
    driver.quit()

# Collects all the bus details data from each route URL we collected
all_buses_data = []

for url in all_routes_urls:
    driver = initialize_driver()
    df = get_bus_details_for_route(driver, url)
    all_buses_data.append(df)
    driver.quit()

# Combine all dataframes into one
final_df = pd.concat(all_buses_data, ignore_index=True)

# Optionally save to a CSV file
final_df.to_csv('bus_details.csv', index=False)

print("Data scraping completed and stored the details to bus_details.csv")
