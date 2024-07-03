from datetime import datetime
from time import sleep

from selenium.common import NoSuchElementException, ElementClickInterceptedException, ElementNotInteractableException, \
    TimeoutException, StaleElementReferenceException
from selenium.webdriver import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine
import pandas as pd
from .driver_init import init_firefox_driver
from airflow.models import Variable
db_name = Variable.get("DB_NAME")


def close_sign_in_info_dialog(driver):
    try:
        sign_in_info_dialog = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div[role='dialog']")))
        dismiss_button = sign_in_info_dialog.find_element(By.CSS_SELECTOR, "button[aria-label='Dismiss sign-in info.']")
        dismiss_button.click()
    except (NoSuchElementException, TimeoutException) as e:
        # print(e)
        pass


def select_destination(driver, search_form, destination):
    try:
        destination_container = search_form.find_element(By.CSS_SELECTOR, "div[data-testid='destination-container']")

        destination_input = destination_container.find_element(By.CSS_SELECTOR, "input[name='ss']")
        destination_input.send_keys(destination)

        autocomplete = WebDriverWait(destination_container, 10).until(
            EC.visibility_of_element_located((By.ID, "autocomplete-results")))
        autocomplete_results = autocomplete.find_elements(By.CSS_SELECTOR, "div[data-testid='autocomplete-result'")
        for result in autocomplete_results:
            if result.text.split()[0] == destination:
                driver.execute_script("arguments[0].click();", result)
                break
        return True
    except (NoSuchElementException, ElementNotInteractableException, ElementClickInterceptedException) as e:
        print(e)
        print("Error selecting destination!")
        return False


def select_month(date_picker, month):
    try:
        left_calendar = date_picker.find_element(By.TAG_NAME, "div")
        while left_calendar.find_element(By.TAG_NAME, "h3").text != month:
            date_picker.find_element(By.CSS_SELECTOR, "button[aria-label='Next month']").click()
            left_calendar = date_picker.find_element(By.TAG_NAME, "div")

        return left_calendar

    except (NoSuchElementException, ElementClickInterceptedException) as e:
        print(e)
        print("Error selecting month!")


def select_date(date_picker, month, date):
    try:
        calendar = select_month(date_picker, month).find_element(By.TAG_NAME, "table")
        for cell in calendar.find_elements(By.CSS_SELECTOR, "td[role='gridcell']"):
            if cell.text == date:
                cell.click()
                break

    except (NoSuchElementException, ElementClickInterceptedException) as e:
        print("Error selecting date!")


def select_checkin_checkout(driver, search_form, check_in, check_out):
    check_in_month, check_in_day = check_in.strftime("%B") + " " + str(check_in.year), str(check_in.day)
    check_out_month, check_out_day = check_out.strftime("%B") + " " + str(check_out.year), str(check_out.day)

    try:
        try:
            date_picker = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-testid='searchbox-datepicker-calendar']")))
        except TimeoutException as e:
            print(e)
            search_form.find_element(By.CSS_SELECTOR, "div[data-testid='searchbox-dates-container']").click()
            date_picker = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-testid='searchbox-datepicker-calendar']")))

        select_date(date_picker, check_in_month, check_in_day)
        select_date(date_picker, check_out_month, check_out_day)
        return True
    except (NoSuchElementException, ElementClickInterceptedException, TimeoutException) as e:
        print("Error selecting checkin checkout!")
        # print(e)
        return False


def search(driver, destination, check_in, check_out):
    try:
        close_sign_in_info_dialog(driver)

        search_form = driver.find_element(By.CSS_SELECTOR, "div[data-testid='searchbox-layout-wide']")

        if not select_destination(driver, search_form, destination):
            return False

        if not select_checkin_checkout(driver, search_form, check_in, check_out):
            return False

        search_form.find_element(By.CSS_SELECTOR, "button[type='submit']").click()

        return True
    except (NoSuchElementException, ElementClickInterceptedException) as e:
        print("Search failed!")
        return False


def get_rooms_info(driver):
    try:
        accommodations_table = WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.ID, "available_rooms")))
        rows = accommodations_table.find_element(By.TAG_NAME, "tbody").find_elements(By.TAG_NAME, "tr")
        accommodations = []
        for row in rows:
            cols = row.find_elements(By.TAG_NAME, "td")
            try:
                accommodation_name = cols[0].find_element(By.CLASS_NAME, "hprt-roomtype-block").text

                try:
                    availability = cols[0].find_element(By.CLASS_NAME, "thisRoomAvailabilityNew").text
                    # raise NoSuchElementException
                except NoSuchElementException:
                    availability = "Available"

                try:
                    pension = cols[2].find_element(By.CLASS_NAME, "bui-list__description").text
                except NoSuchElementException:
                    pension = ""
                try:
                    annulation = cols[3].find_element(By.CSS_SELECTOR, "div[data-testid='policy-subtitle']").text
                except NoSuchElementException:
                    annulation = ""

                accommodations.append({
                    "accommodation_name": accommodation_name,
                    "number_of_guests": len(cols[0].find_elements(By.TAG_NAME, "i")),
                    "availability": availability,
                    "price": cols[2].find_element(By.CLASS_NAME, "bui-price-display__value").text,
                    "pension": pension,
                    "annulation": annulation
                })

            except NoSuchElementException as e:
                try:
                    pension = cols[2].find_element(By.CLASS_NAME, "bui-list__description").text
                except NoSuchElementException:
                    pension = ""

                try:
                    annulation = cols[3].find_element(By.CSS_SELECTOR, "div[data-testid='policy-subtitle']").text
                except NoSuchElementException:
                    annulation = ""

                accommodations.append({
                    "accommodation_name": accommodations[-1]["accommodation_name"],
                    "number_of_guests": len(cols[0].find_elements(By.TAG_NAME, "i")),
                    "availability": accommodations[-1]["availability"],
                    "price": cols[1].find_element(By.CLASS_NAME, "bui-price-display__value").text,
                    "pension": pension,
                    "annulation": annulation
                })
        return accommodations
    except NoSuchElementException as e:
        print("ERROR getting accommodations info!")
        return []


def get_hotel_info(driver):
    try:
        hotel_page = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.CLASS_NAME, "hotelchars")))
        hotel_name = hotel_page.find_element(By.CLASS_NAME, "pp-header__title").text

        try:
            hotel_stars = len(
                hotel_page.find_element(By.CSS_SELECTOR, "div[data-testid='quality-rating']").find_elements(By.TAG_NAME,
                                                                                                            "svg"))
        except NoSuchElementException:
            hotel_stars = 0

        accommodations = get_rooms_info(driver)
        hotel_info = []
        for accommodation in accommodations:
            room_type, capacity, availability, price, pension, annulation = accommodation.values()
            if capacity == 2:
                hotel_info.append(
                    {
                        "name": hotel_name,
                        "stars": hotel_stars,
                        "room_type": room_type,
                        "pension": pension,
                        "annulation": annulation,
                        "price": price,
                        "availability": availability
                    }
                )
        return hotel_info
    except (NoSuchElementException, TimeoutException, ElementClickInterceptedException, StaleElementReferenceException):
        print("ERROR getting hotel info!")
        return []


def select_hotels_filter(driver):
    try:
        filters = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-testid='filters-sidebar']")))

        hotels_filter = WebDriverWait(filters, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='ht_id=204']")))
        print(hotels_filter.get_attribute("aria-label").split()[1])
        hotels_filter.click()
        driver.implicitly_wait(2)
    except (TimeoutException, NoSuchElementException, ElementClickInterceptedException) as e:
        print("ERROR selecting filter!")


def load_hotels_list(driver, last_elm):
    try:
        driver.execute_script("arguments[0].scrollIntoView(true);", last_elm)
        hotels_list = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div[aria-label='Property']")))
        return hotels_list
    except TimeoutException:
        print("ERROR loading hotels list!")


def get_hotels_list(driver):
    try:
        sleep(5)
        hotels_list = WebDriverWait(driver, 10).until(
            EC.visibility_of_all_elements_located((By.CSS_SELECTOR, "div[aria-label='Property']")))

        driver.execute_script("arguments[0].scrollIntoView(true);", hotels_list[-1])
        sleep(10)

        hotels_list = WebDriverWait(driver, 10).until(
            EC.visibility_of_all_elements_located((By.CSS_SELECTOR, "div[aria-label='Property']")))

        hotel_links = [hotel_elm.find_element(By.TAG_NAME, "a").get_attribute("href") for hotel_elm in hotels_list]

        return hotel_links
    except (TimeoutException, NoSuchElementException, StaleElementReferenceException) as e:
        print(e)
        print("ERROR getting hotels list!")
        return []


def extract_all_hotels_info(driver):
    hotel_links = get_hotels_list(driver)
    print(f">> Found {len(hotel_links)} hotel.")

    hotels_info = []
    for hotel_link in hotel_links:
        driver.execute_script("window.open();")
        driver.switch_to.window(driver.window_handles[1])
        driver.get(hotel_link)

        hotels_info.append(get_hotel_info(driver))

        driver.close()
        driver.switch_to.window(driver.window_handles[0])

    return hotels_info


def scrap(destination, check_in, check_out):
    driver = init_firefox_driver()

    print(f"Scraping Booking.com for hotels in {destination}")
    driver.get("https://www.booking.com/")
    print("Searching...")
    if search(driver, destination, check_in, check_out):
        print("Search success.")

        print("Scrapping...")
        hotels_info = extract_all_hotels_info(driver)
        if len(hotels_info) == 0:
            driver.refresh()
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
            df.to_sql('bookingcom_src', engine, if_exists='append', index=False, chunksize=100000)
            print("Saved!")
        else:
            raise ValueError("No data returned, retrying...")
    else:
        raise ValueError("Search failed, retrying...")
