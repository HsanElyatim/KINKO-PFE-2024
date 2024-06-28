from datetime import datetime
from time import sleep

import pandas as pd
from selenium.webdriver.common.by import By
from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine
from selenium.common import NoSuchElementException, TimeoutException, ElementClickInterceptedException, \
    StaleElementReferenceException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from .driver_init import init_firefox_driver

from airflow.models import Variable
db_name = Variable.get("DB_NAME")


def select_destination(driver, search_form, destination):
    try:
        destination_input = search_form.find_element(By.CSS_SELECTOR, "input[data-selenium='textInput']")
        destination_input.send_keys(destination)

        autocomplete_panel = WebDriverWait(search_form, 10).until(EC.visibility_of_element_located(
            (By.CSS_SELECTOR, "div[data-selenium='autocompletePanel']")
        ))
        autosuggest_items = autocomplete_panel.find_elements(By.CSS_SELECTOR,
                                                             "li[data-selenium='autosuggest-item']")
        for autosuggest_item in autosuggest_items:
            if autosuggest_item.get_attribute("data-text") == destination:
                driver.execute_script("arguments[0].click();", autosuggest_item)
                break
    except (NoSuchElementException, TimeoutException, ElementClickInterceptedException) as e:
        print("ERROR selecting destination!")


def select_month(driver, range_picker, month):
    try:
        left_calendar = range_picker.find_element(By.CLASS_NAME, "DayPicker-Month")

        while left_calendar.find_element(By.CLASS_NAME, "DayPicker-Caption").text != month:
            target_month_number = datetime.strptime(month, "%B %Y").month
            current_month_number = datetime.strptime(
                left_calendar.find_element(By.CLASS_NAME, "DayPicker-Caption").text, "%B %Y").month
            if current_month_number < target_month_number:
                driver.execute_script("arguments[0].click();",
                                      range_picker.find_element(By.CSS_SELECTOR, "button[aria-label='Next Month']"))
            else:
                driver.execute_script("arguments[0].click();",
                                      range_picker.find_element(By.CSS_SELECTOR, "button[aria-label='Previous Month']"))
            left_calendar = range_picker.find_element(By.CLASS_NAME, "DayPicker-Month")
        return left_calendar
    except (NoSuchElementException, ElementClickInterceptedException):
        print("ERROR selecting month!")


def select_date(driver, range_picker, day, month):
    try:
        calendar = select_month(driver, range_picker, month)
        days_picker = calendar.find_elements(By.CLASS_NAME, "PriceSurgePicker-Day")
        for day_picker in days_picker:
            if day_picker.text == day:
                driver.execute_script("arguments[0].click();", day_picker)
                break
    except (NoSuchElementException, ElementClickInterceptedException) as e:
        # print(e)
        print("ERROR selecting date!")


def select_checkin_checkout(driver, search_form, check_in, check_out):
    check_in_month, check_in_day = check_in.strftime("%B") + " " + str(check_in.year), str(check_in.day)
    check_out_month, check_out_day = check_out.strftime("%B") + " " + str(check_out.year), str(check_out.day)

    try:
        range_picker = WebDriverWait(search_form, 10).until(EC.visibility_of_element_located(
            (By.CSS_SELECTOR, "div[data-selenium='rangePickerCheckIn']")
        ))

        select_date(driver, range_picker, check_in_day, check_in_month)
        select_date(driver, range_picker, check_out_day, check_out_month)
    except TimeoutException:
        print("ERROR selecting check-in check-out!")


def search(driver, destination, check_in, check_out):
    try:
        search_form = driver.find_element(By.CSS_SELECTOR, "div[data-selenium='searchBox']")
        select_destination(driver, search_form, destination)
        select_checkin_checkout(driver, search_form, check_in, check_out)
        driver.execute_script(
            "arguments[0].click();",
            driver.find_element(By.CSS_SELECTOR, "button[data-selenium='searchButton']")
        )

        return True
    except (NoSuchElementException, ElementClickInterceptedException) as e:
        # print(e)
        print("Search failed!")
        return False


def get_hotel_link(driver, hotel_elm):
    try:
        driver.execute_script("arguments[0].scrollIntoView(true);", hotel_elm)
        # print(hotel_elm.find_element(By.CSS_SELECTOR, "h3[data-selenium='hotel-name']").text)
        return hotel_elm.find_element(By.TAG_NAME, "a").get_attribute("href")
    except (NoSuchElementException, StaleElementReferenceException) as e:
        print("ERROR getting hotel link!")


def get_room_offer_info(room_offer):
    try:
        benefits_container = room_offer.find_element(By.CLASS_NAME, "ChildRoomsList-roomCell-featureBuckets")
        benefits = benefits_container.find_elements(By.CSS_SELECTOR, "div[data-selenium='ChildRoomList-roomFeature']")

        capacity = len(room_offer.find_element(By.CLASS_NAME, "ChildRoomsList-roomCell-capacity")
                       .find_elements(By.TAG_NAME, "i"))

        price = room_offer.find_element(By.CSS_SELECTOR, "span[data-selenium='PriceDisplay']").text

        availability = room_offer.find_element(By.CSS_SELECTOR, "div[data-selenium='ChildRoomsList-urgency']").text

        return {
            "benefits": [benefit.text for benefit in benefits],
            "capacity": capacity,
            "price": price,
            "availability": availability
        }
    except (NoSuchElementException, StaleElementReferenceException) as e:
        # print(e)
        print("ERROR getting room offer info!")
        return None


def get_room_info(room_elm):
    try:
        room_type = room_elm.find_element(By.CSS_SELECTOR, "span[data-selenium='masterroom-title-name']").text

        try:
            room_offers_container = room_elm.find_element(By.CLASS_NAME, "MasterRoom-roomsList") \
                .find_element(By.CLASS_NAME, "ChildRoomsList")
        except NoSuchElementException as e:
            print("No rooms found!")
            return []
        room_offers = room_offers_container.find_elements(By.CLASS_NAME, "ChildRoomsList-room")
        # print(f"{len(room_offers)} room offers found")

        room_info = []
        for room_offer in room_offers:
            room_offer_info = get_room_offer_info(room_offer)
            if room_offer_info is not None:
                benefits, capacity, price, availability = room_offer_info.values()

                room_info.append(
                    {
                        "room_type": room_type,
                        "benefits": benefits,
                        "capacity": capacity,
                        "price": price,
                        "availability": availability
                    }
                )

        return room_info
    except NoSuchElementException as e:
        print(e)
        print("ERROR getting room info!")
        return []


def get_hotel_info(driver):
    try:
        property_content = WebDriverWait(driver, 10) \
            .until(EC.presence_of_element_located((By.ID, "property-main-content")))

        hotel_name = property_content.find_element(By.CSS_SELECTOR, "p[data-selenium='hotel-header-name']").text

        hotel_stars = len(property_content.find_element(By.CLASS_NAME, "star-rating")
                          .find_elements(By.TAG_NAME, "svg"))

        try:
            room_grid = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "roomGrid")))
        except TimeoutException as e:
            driver.refresh()
            room_grid = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "roomGrid")))

        rooms = room_grid.find_elements(By.CSS_SELECTOR, "div[data-selenium='MasterRoom']")
        # print(f"{len(rooms)} room types found")

        hotel_info = []
        for room in rooms:
            for room_info in get_room_info(room):
                room_type, benefits, capacity, price, availability = room_info.values()
                if capacity == 2:
                    hotel_info.append(
                        {
                            "name": hotel_name,
                            "stars": hotel_stars,
                            "room_type": room_type,
                            "pension": "",
                            "annulation": benefits[0],
                            "price": price,
                            "availability": availability
                        }
                    )

        return hotel_info
    except NoSuchElementException as e:
        # print(e)
        print("ERROR getting hotel info!")
        return []


def load_more_to_hotels_list(driver):
    try:
        sleep(5)
        hotels_list_containers = driver.find_elements(By.CLASS_NAME, 'hotel-list-container')

        hotels_list = hotels_list_containers[1].find_elements(By.CSS_SELECTOR, "li[data-selenium='hotel-item']")

        driver.execute_script("arguments[0].scrollIntoView(true);",
                              driver.find_element(By.CLASS_NAME, "Footer"))

        return hotels_list
    except NoSuchElementException as e:
        print("ERROR loading more to hotels list!")


def get_hotels_list(driver):
    try:
        sleep(5)
        hotel_filter = driver.find_element(By.CSS_SELECTOR,
                                           "span[data-component='search-filter-accommodationtype'][aria-label='Hotel']")
        driver.execute_script("arguments[0].click();", hotel_filter)

        hotels_list_container = WebDriverWait(driver, 10) \
            .until(EC.visibility_of_element_located((By.CLASS_NAME, 'hotel-list-container')))

        hotels_list = WebDriverWait(hotels_list_container, 10) \
            .until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "li[data-selenium='hotel-item']")))
        hotel_links = [get_hotel_link(driver, hotel_elm) for hotel_elm in hotels_list]

        driver.execute_script("arguments[0].scrollIntoView(true);", hotels_list[-1])
        sleep(2)
        driver.execute_script("arguments[0].scrollIntoView(true);",
                              driver.find_element(By.CLASS_NAME, "Footer"))

        sleep(10)
        hotels_list_containers = driver.find_elements(By.CLASS_NAME, 'hotel-list-container')

        hotels_list2 = WebDriverWait(hotels_list_containers[1], 10) \
            .until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "li[data-selenium='hotel-item']")))
        hotel_links2 = [get_hotel_link(driver, hotel_elm) for hotel_elm in hotels_list2]

        hotel_links.extend(hotel_links2)

        return hotel_links
    except (NoSuchElementException, TimeoutException) as e:
        # print(e)
        print("ERROR getting hotels list!")
        return []


def extract_all_hotels_info(driver):
    hotel_links = get_hotels_list(driver)
    filtered_hotel_links = [elem for elem in hotel_links if elem is not None]
    print(f">> Found {len(hotel_links)} hotel...")

    hotels_info = []
    if len(filtered_hotel_links) > 0:
        for link in filtered_hotel_links:
            driver.execute_script("window.open();")
            driver.switch_to.window(driver.window_handles[1])
            driver.get(link)
            hotels_info.append(get_hotel_info(driver))
            driver.close()
            driver.switch_to.window(driver.window_handles[0])
    else:
        print("No hotel found.")

    return hotels_info


def scrap(destination, check_in, check_out):
    driver = init_firefox_driver()

    print(f"Scraping Agoda for hotels in {destination}")
    driver.get("https://www.agoda.com/")
    print("Searching...")
    if search(driver, destination, check_in, check_out):
        print("Search Complete.")

        print("Extracting...")
        hotels_info = extract_all_hotels_info(driver)

        driver.quit()

        if len(hotels_info) != 0:
            print("Scrap success.")

            flattened_hotels_info = [item for sublist in hotels_info for item in sublist]
            df = pd.DataFrame(flattened_hotels_info)
            df['check_in'] = check_in
            df['extracted_at'] = datetime.today().date()

            print("Saving to DB...")
            conn = BaseHook.get_connection(db_name)
            engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
            df.to_sql('agoda_src', engine, if_exists='append', index=False, chunksize=100000)
            print("Saved!")
        else:
            raise ValueError("No data returned, retrying...")
    else:
        raise ValueError("Search failed, retrying...")
