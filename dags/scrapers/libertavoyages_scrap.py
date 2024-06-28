from selenium.common import NoSuchElementException, WebDriverException, ElementClickInterceptedException, \
    TimeoutException
from selenium.webdriver.common.by import By
from time import sleep
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from dags.scrapers.dictionary import month_names_en_fr, month_mapping
from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine
import pandas as pd
from driver_init import init_firefox_driver
from datetime import datetime
from airflow.models import Variable
db_name = Variable.get("DB_NAME")


def select_destination(search_form, destination):
    try:
        destination_input = search_form.find_element(By.CLASS_NAME, "form-group").find_element(By.TAG_NAME, "input")
        destination_input.send_keys(destination)
        search_form.find_element(By.CLASS_NAME, "form-group").find_element(By.XPATH, "ul/li").click()

        return True
    except (NoSuchElementException, WebDriverException):
        print(f"Destination {destination} not found!")
        return False


def transform_month(month):
    month_abbr, year = month.split(' ')
    full_month = month_mapping.get(month_abbr)
    return f"{full_month} {year}"


def select_date(driver, month, day):
    try:
        calendar_container = driver.find_element(By.CLASS_NAME, "daterangepicker")
        calendar = calendar_container.find_element(By.CLASS_NAME, "left")

        while transform_month(calendar.find_element(By.CLASS_NAME, "month").text) != month:
            driver.find_element(By.CLASS_NAME, "daterangepicker").find_element(By.CLASS_NAME, "right").find_element(
                By.CLASS_NAME, "next").click()
            calendar = driver.find_element(By.CLASS_NAME, "daterangepicker").find_element(By.CLASS_NAME, "left")

        calendar_body = calendar.find_element(By.TAG_NAME, "tbody")
        days_elements = calendar_body.find_elements(By.TAG_NAME, "td")
        for day_el in days_elements:
            if day_el.text == day:
                day_el.click()
                break

        return True
    except NoSuchElementException:
        print("Error handling date selection!")
        return False


def select_checkin_checkout(driver, check_in, check_out, search_form):
    check_in_month, check_in_day = month_names_en_fr.get(check_in.strftime("%B")) + " " + str(check_in.year), str(
        check_in.day)
    check_out_month, check_out_day = month_names_en_fr.get(check_out.strftime("%B")) + " " + str(check_out.year), str(
        check_out.day)

    try:
        search_form.find_element(By.CSS_SELECTOR, "input[name='dates']").click()
        select_date(driver, check_in_month, check_in_day)
        select_date(driver, check_out_month, check_out_day)

        driver.find_element(By.CLASS_NAME, "daterangepicker").find_element(By.CLASS_NAME, "drp-buttons").find_element(
            By.CLASS_NAME, "applyBtn").click()

        return True
    except NoSuchElementException:
        print("Selecting checkin checkout failed!")
        return False


def search(driver, destination, check_in, check_out):
    try:
        search_form = driver.find_element(By.ID, "search_bar")

        select_destination(search_form, destination)

        select_checkin_checkout(driver, check_in, check_out, search_form)

        search_form.find_element(By.CLASS_NAME, "frorm-group").find_element(By.TAG_NAME, "button").click()

        return True
    except NoSuchElementException:
        print("Search failed!")
        return False


def get_pensions_list(hotel):
    try:
        return [el.text for el in hotel.find_element(By.CLASS_NAME, "liste-pensions").find_elements(By.TAG_NAME, "a")]
    except NoSuchElementException:
        print("Error getting pensions!")
        return []


def get_rooms_list(hotel):
    pensions = get_pensions_list(hotel)
    try:
        rooms_per_pensions = hotel.find_element(By.CLASS_NAME, "tab-content").find_elements(By.CLASS_NAME, "tab-pane")
        rooms_list = []
        for pension in pensions:
            for rooms_per_pension in rooms_per_pensions:
                rooms_list.append((pension, rooms_per_pension))

        return rooms_list
    except NoSuchElementException:
        print("Error getting rooms list!")
        return []


def get_rooms_info(hotel):
    try:
        rooms_per_pensions = get_rooms_list(hotel)

        rooms_info = []
        for el in rooms_per_pensions:
            rooms = el[1].find_elements(By.TAG_NAME, 'option')
            for room in rooms:
                room_name = room.get_attribute("data-libelle")
                room_price = room.get_attribute("data-tarif")
                availability = hotel.find_element(By.TAG_NAME, "button").text

                rooms_info.append({
                    "name": room_name,
                    "pension": el[0],
                    "price": room_price,
                    "availability": availability
                })

        return rooms_info
    except NoSuchElementException:
        print("Error getting rooms info!")
        return []


def get_hotel_info(hotel):
    try:
        hotel_desc = hotel.find_element(By.CLASS_NAME, "desc")
        hotel_name = hotel_desc.find_element(By.TAG_NAME, "h3").text.strip()

        try:
            hotel_stars = len(hotel_desc.find_elements(By.TAG_NAME, "img"))
        except NoSuchElementException:
            hotel_stars = hotel_desc.find_elements(By.TAG_NAME, "strong").text.spli(' ')[0]

        hotel_info = []
        for room in get_rooms_info(hotel):
            hotel_info.append({
                "name": hotel_name,
                "stars": hotel_stars,
                "room_type": room["name"],
                "pension": room["pension"],
                "availability": room["availability"],
                "annulation": "",
                "price": room["price"]
            })

        return hotel_info
    except (NoSuchElementException, ElementClickInterceptedException) as e:
        print("Failed extracting hotel info!")
        return []


def extract_hotels_list(driver):
    try:
        WebDriverWait(driver, 10).until(EC.invisibility_of_element((By.CLASS_NAME, "loading")))

        sleep(10)

        hotels_list = driver.find_element(By.ID, "load_hotels_wrapper").find_elements(By.CLASS_NAME, "hotel_div")
        print(f">> {len(hotels_list)} hotel found.")

        return hotels_list
    except (TimeoutException, NoSuchElementException):
        print("No hotels found")
        return []


def extract_all_hotels_info(driver):
    hotels_infos = []
    for hotel in extract_hotels_list(driver):
        hotels_infos.append(get_hotel_info(hotel))
    return hotels_infos


def scrap(destination, check_in, check_out):
    driver = init_firefox_driver()

    print(f"Scraping Liberta for hotels in {destination}")
    driver.get("https://www.libertavoyages.com/")
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
            df.to_sql('libertavoyages_src', engine, if_exists='append', index=False, chunksize=100000)
            print("Saved!")
        else:
            raise ValueError("No data returned, retrying...")
    else:
        raise ValueError("Search failed, retrying...")
