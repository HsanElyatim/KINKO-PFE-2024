from datetime import datetime
from time import sleep
import pandas as pd
from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine

from selenium.common import TimeoutException, NoSuchElementException, ElementClickInterceptedException, \
    StaleElementReferenceException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

from .utils import extract_hotel_number
from .dictionary import month_names_en_fr

from .driver_init import init_firefox_driver
from airflow.models import Variable
db_name = Variable.get("DB_NAME")


def select_date(date_container, date):
    try:
        month_year = month_names_en_fr.get(date.strftime("%B")) + " " + str(date.year)
        day = str(date.day)

        date_pickers = date_container.find_elements(By.CLASS_NAME, "drp-calendar")
        while date_pickers[0].find_element(By.CLASS_NAME, "month").text != month_year:
            date_pickers[1].find_element(By.CLASS_NAME, "next").click()
        if date_pickers[0].find_element(By.CLASS_NAME, "month").text == month_year:
            dates = date_pickers[0].find_elements(By.XPATH, "//td")
            for date_el in dates:
                if date_el.text == day:
                    date_el.click()
                    break
    except NoSuchElementException:
        print("Error selecting date!")


def search(driver, destination, arr_date, dep_date):
    try:
        # Find the hotel search form
        form = driver.find_element(By.ID, "hotel")

        # Find the destination input field within the form and click it
        destination_input = form.find_element(By.ID, "search")
        destination_input.click()

        # Wait for the destination list to appear
        dest_list = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "liste_dest")))

        # simulate destination list not appearing
        # dest_list = wait.until(EC.presence_of_element_located((By.ID, "liste_destX")))

        # Find all destination list items
        dest_elements = dest_list.find_elements(By.XPATH, "//li[@id='list_dest']")

        # Click on the destination element matching the provided destination
        for element in dest_elements:
            if element.text == destination:
                element.click()
                break

        date_containers = driver.find_elements(By.CLASS_NAME, "daterangepicker")
        select_date(date_containers[0], arr_date)
        select_date(date_containers[1], dep_date)

        # Click the "close" button on the form
        close_button_el = form.find_element(By.CLASS_NAME, "fermer_ch1")
        close_button_el.click()

        # Click the search button
        driver.execute_script("recherche_y();")

        return True
    except (TimeoutException, NoSuchElementException, ElementClickInterceptedException) as e:
        print("Search Failed!")
        return False


def extract_pensions(hotel_el):
    try:
        pensions_el_container = WebDriverWait(hotel_el, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "pension")))
        pensions_el = pensions_el_container.find_elements(By.TAG_NAME, "div")
        if len(pensions_el) < 1:
            return None
        pension_dict = {}
        for pension in pensions_el:
            if (pension.get_attribute("id") != ""
                    and pension.text != ""
                    and pension.get_attribute("id") not in pension_dict.keys()):
                pension_dict[pension.get_attribute("id")] = pension.text
        return pension_dict
    except (NoSuchElementException, TimeoutException):
        print("No pensions found!")


def get_rooms_info(hotel_el, h_id):
    try:
        # hotel_el.find_element(By.CLASS_NAME, "lien_call_prix").click()

        pension_dict = extract_pensions(hotel_el)
        if pension_dict is None:
            return []
        room_types = {}
        for room in hotel_el.find_element(By.ID, f"div_pension1_{list(pension_dict.keys())[0]}_{h_id}").find_elements(
                By.TAG_NAME, "label"):
            key = room.find_element(By.TAG_NAME, "input").get_attribute("id")
            value = room.find_element(By.CLASS_NAME, "span_lib_ch").text

            if key not in room_types.keys() and value != "":
                room_types[key] = value
        # print(room_types)
        rooms_info = []
        for pension_short, pension_name in pension_dict.items():
            rooms_container = hotel_el.find_element(By.ID, f"div_pension1_{pension_short}_{h_id}")
            room_elements = rooms_container.find_elements(By.TAG_NAME, "label")

            for room in room_elements:
                key = room.find_element(By.TAG_NAME, "input").get_attribute("id")
                value = room.find_element(By.CLASS_NAME, "span_lib_ch").text.strip()
                if key not in room_types.keys() and value != "":
                    room_types[key] = value

                availability_message = room.find_element(By.XPATH, '..').find_element(By.CLASS_NAME,
                                                                                      "span_stock").text.strip()
                availability = True if "Disponible" in availability_message else False

                price = room.find_element(By.TAG_NAME, "input").get_attribute("value")

                rooms_info.append({
                    'name': value if value != "" else "Unknown",
                    "pension": pension_name,
                    "availability": str(availability) + ", " + availability_message,
                    'price': price
                })

        # print(rooms)
        return rooms_info
    except NoSuchElementException as e:
        print("Error getting rooms info", e)
        return []


def get_hotel_info(driver, hotel_id):
    try:
        hotel_el = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, hotel_id)))
        driver.execute_script("arguments[0].scrollIntoView(true);", hotel_el)

        h_id = hotel_el.get_attribute("id")
        h_id = extract_hotel_number(h_id)
        name = hotel_el.find_element(By.ID, "libelle_hotel").text
        try:
            stars = hotel_el.find_element(By.CLASS_NAME, "etoliess").text
        except NoSuchElementException:
            stars = 0

        facerd = hotel_el.find_element(By.ID, f"facerd{h_id}")
        try:
            annulation_message = facerd.find_element(By.CLASS_NAME, "annul_gratuit").text.strip()
            annulation = True if "Gratuite" in annulation_message else False
        except NoSuchElementException:
            annulation = False
            annulation_message = "Not Found"

        rooms_info = get_rooms_info(hotel_el, h_id)

        hotel_info = []
        for room in rooms_info:
            hotel_info.append({
                "name": name,
                "stars": stars,
                "room_type": room["name"],
                "pension": room["pension"],
                "availability": room["availability"],
                "annulation": str(annulation) + ", " + annulation_message,
                "price": room["price"]
            })

        return hotel_info
    except (NoSuchElementException, StaleElementReferenceException, TimeoutException):
        print("Error getting hotel info!")
        return []


def extract_hotels_list(driver):
    try:
        result = driver.find_element(By.ID, "ruslt_dispo")
        hotels_el_list = result.find_elements(By.CLASS_NAME, "hotel_y_prom")
        hotels_ids_list = [hotel_el.get_attribute("id") for hotel_el in hotels_el_list]
        print(f">> {len(hotels_ids_list)} hotel found.")
        return hotels_ids_list
    except NoSuchElementException:
        print("No hotels found!")
        return []


def extract_all_hotels_info(driver):
    sleep(10)
    hotels_list = extract_hotels_list(driver)
    hotels_infos = []
    for hotel_id in hotels_list:
        data = get_hotel_info(driver, hotel_id)
        if data is None:
            continue
        hotels_infos.append(data)
    return hotels_infos


def scrap(destination, check_in, check_out):
    driver = init_firefox_driver()
    print(f"Scraping Tunisie Booking for hotels in {destination}")
    driver.get("https://tn.tunisiebooking.com/")
    print("Searching...")
    if search(driver, destination, check_in, check_out):
        print("Search success.")

        print("Scrapping...")
        hotels_info = extract_all_hotels_info(driver)

        driver.quit()

        if len(hotels_info) != 0:
            print("Scrap success.")

            flattened_hotels_info = [item for sublist in hotels_info for item in sublist]
            df = pd.DataFrame(flattened_hotels_info)
            df['destination'] = destination
            df['check_in'] = check_in
            df['extracted_at'] = datetime.today().date()

            print("Saving to DB...")
            conn = BaseHook.get_connection(db_name)
            engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
            df.to_sql('tunisiebooking_src', engine, if_exists='append', index=False, chunksize=100000)
            print("Saved!")
        else:
            raise ValueError("No data returned, retrying...")
    else:
        raise ValueError("Search failed, retrying...")
