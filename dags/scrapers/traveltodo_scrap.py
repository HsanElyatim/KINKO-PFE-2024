from datetime import datetime
from selenium.common import NoSuchElementException, ElementNotInteractableException, TimeoutException, \
    ElementClickInterceptedException
from selenium.webdriver.common.by import By
from time import sleep
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

from dags.scrapers.dictionary import month_names_en_fr
from utils import contains_any
import pandas as pd
from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine
from driver_init import init_firefox_driver
from airflow.models import Variable
db_name = Variable.get("DB_NAME")


def select_destination(driver, search_form, destination):
    """
        Selects the destination in the search form on the webpage.

        Parameters:
            driver (WebDriver): The WebDriver instance.
            search_form (WebElement): The search form WebElement.
            destination (str): The destination city or location to select.

        Returns:
            None
    """
    try:
        # Locate the destination input container
        destination_input_container = search_form.find_element(By.CLASS_NAME, "destination")

        # Find the destination input field within the container
        destination_input = destination_input_container.find_element(By.ID, "locality")

        # Send keys to the destination input field
        destination_input.send_keys(destination)

        # Wait for the destination list container to appear
        destination_list_container = destination_input_container.find_element(By.CLASS_NAME, "tt-dataset-destination")

        # Find all destination list items
        destination_list = destination_list_container.find_elements(By.CLASS_NAME, "tt-suggestion")

        # Click on the first suggestion in the destination list
        driver.execute_script("arguments[0].click();", destination_list[0])
    except (NoSuchElementException, ElementNotInteractableException):
        print("Error selecting destination!")


def select_date(driver, date_picker, date):
    """
        Selects a specific date from a date picker on a web page.

        Args:
            driver (WebDriver): The Selenium WebDriver instance.
            date_picker (WebElement): The WebElement representing the date picker.
            date (datetime.date): The date to be selected.

        Returns:
            None
    """
    try:
        # Get the month and day from the provided date
        month = month_names_en_fr.get(date.strftime("%B"))
        day = str(date.day)
        # Scroll the date picker into view
        driver.execute_script("arguments[0].scrollIntoView(true);", date_picker)

        # Find the calendar container
        calendar_containers = date_picker.find_elements(By.CLASS_NAME, "pika-lendar")
        title = calendar_containers[0].find_element(By.CLASS_NAME, "pika-title")

        # Select the month
        month_select = title.find_element(By.CLASS_NAME, "pika-select-month")
        month_select.click()
        months_to_select = month_select.find_elements(By.TAG_NAME, "option")
        for month_to_select in months_to_select:
            if month_to_select.text == month:
                # raise ElementNotInteractableException
                month_to_select.click()
                break

        # Find and click the day element
        calendar_containers = date_picker.find_elements(By.CLASS_NAME, "pika-lendar")
        calendar_body = calendar_containers[0].find_element(By.TAG_NAME, "tbody")
        days_elements = calendar_body.find_elements(By.TAG_NAME, "td")
        for day_el in days_elements:
            if day_el.get_attribute("data-day") == day:
                day_el.click()
                break
    except (NoSuchElementException, ElementNotInteractableException):
        print("Error selecting date!")


def search(driver, destination, arr_date, dep_date):
    """
        Performs a hotel search on a booking website.

        Args:
            driver (WebDriver): The Selenium WebDriver instance.
            destination (str): The destination to search for.
            arr_date (datetime.date): The arrival date.
            dep_date (datetime.date): The departure date.

        Returns:
            bool: True if the search was successful, False otherwise.
    """
    try:
        # Find the search form
        search_form = driver.find_element(By.ID, "searchForm")

        # Select destination
        select_destination(driver, search_form, destination)

        # Select arrival and departure dates
        date_pickers = driver.find_elements(By.CLASS_NAME, "pika-single")
        select_date(driver, date_pickers[0], arr_date)
        select_date(driver, date_pickers[1], dep_date)

        # Click on the search button
        search_btn = search_form.find_element(By.CLASS_NAME, "fas")
        search_btn.click()
        return True
    except (NoSuchElementException, ElementNotInteractableException) as e:
        print("Search Failed!")
        return False


def extract_hotels_list(driver):
    """
        Retrieves a list of hotels from the search results page.

        Args:
            driver (WebDriver): The Selenium WebDriver instance.

        Returns:
            list: A list of WebElement representing hotels.
    """
    try:
        # Wait until the loading indicator disappears
        WebDriverWait(driver, 10).until(EC.invisibility_of_element_located((By.ID, "loading-result")))

        # Scroll to the footer
        footer = driver.find_element(By.TAG_NAME, "footer")
        driver.execute_script("arguments[0].scrollIntoView(true);", footer)

        # Wait for a while to ensure all elements are loaded
        sleep(10)

        # Find the results container and extract hotel cards
        results = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "results")))
        hotels_list = results.find_elements(By.CLASS_NAME, "card")
        print(f">> {len(hotels_list)} hotel found.")
        return hotels_list
    except (NoSuchElementException, TimeoutException) as e:
        print("Error getting hotels list!")
        return []


def extract_rooms_info(hotel):
    """
        Extracts room information from a hotel WebElement.

        Args:
            hotel (WebElement): The WebElement representing the hotel.

        Returns:
            list: A list of dictionaries containing room information.
    """
    try:
        # Click the info button to expand room details
        info_btn = hotel.find_element(By.CLASS_NAME, "link-search")
        info_btn.click()

        # Find the container for room information
        container = hotel.find_element(By.CLASS_NAME, "lstrooms")
        rooms = container.find_elements(By.CLASS_NAME, "item-room")

        rooms_info = []
        for room in rooms:
            # Extract room name
            name_x = room.find_element(By.CLASS_NAME, "mb-2").text.strip().split(" ")
            name = []
            for i in name_x[2::]:
                if contains_any(i, ["disponible", "complet", "sur", "non", "minimum"]):
                    break
                name.append(i)
            name = " ".join(name)

            # Check room availability
            availability_message = (room.find_element(By.CLASS_NAME, "badge").text.strip())
            availability = "True" if availability_message == "Disponible" else "False"

            # Check if cancellation is free
            annulation_message = room.find_element(By.CLASS_NAME, "rateDescription").text.strip()
            annulation = "True" if "gratuite" in annulation_message else "False"

            # Iterate over pension options
            pension_select = room.find_element(By.TAG_NAME, "select")
            pension_list = pension_select.find_elements(By.TAG_NAME, "option")
            for pension in pension_list:
                pension.click()
                price = room.find_element(By.CLASS_NAME, "price").get_attribute("value").strip().split()[0]
                rooms_info.append({
                    "name": name,
                    "pension": pension.text,
                    "price": price,
                    "availability": availability + ", " + availability_message,
                    "annulation": annulation + ", " + annulation_message
                })

        return rooms_info
    except (NoSuchElementException, ElementClickInterceptedException, ElementNotInteractableException) as e:
        print("Error getting rooms info!")
        return []


def get_hotel_info(driver, hotel):
    """
        Extracts information about a hotel including its name, star rating, and room details.

        Args:
            driver (WebDriver): The Selenium WebDriver instance.
            hotel (WebElement): The WebElement representing the hotel.

        Returns:
            list: A list of dictionaries containing hotel information.
    """
    try:
        # Scroll to the hotel element
        driver.execute_script("arguments[0].scrollIntoView();", hotel)

        # Extract hotel name
        name = hotel.find_element(By.CLASS_NAME, "h3").text.strip()

        # Extract hotel star rating
        stars = len(hotel.find_element(By.CLASS_NAME, "h3").find_elements(By.TAG_NAME, "i"))

        # Extract room information
        rooms_info = extract_rooms_info(hotel)
        hotel_info = []
        for room in rooms_info:
            hotel_info.append({
                "name": name,
                "stars": stars,
                "room_type": room["name"],
                "pension": room["pension"],
                "availability": room["availability"],
                "annulation": room["annulation"],
                "price": room["price"]
            })
        return hotel_info
    except NoSuchElementException:
        print("Error getting hotel info!")
        return []


def extract_all_hotels_info(driver):
    """
    Extracts information for all hotels listed on a page.

    Args:
        driver (WebDriver): The Selenium WebDriver instance.

    Returns:
        list: A list of dictionaries containing information about each hotel.
    """
    hotels_infos = []
    for hotel in extract_hotels_list(driver):
        hotels_infos.append(get_hotel_info(driver, hotel))
    return hotels_infos


def scrap(destination, check_in, check_out):
    """
        Scrapes Travel To Do website for hotels information based on the provided destination, check-in, and check-out dates.

        Parameters:
            destination (str): The destination city or location.
            check_in (datetime.date): The check-in date.
            check_out (datetime.date): The check-out date.

        Returns:
            None
    """
    driver = init_firefox_driver()

    print(f"Scraping Travel To Do for hotels in {destination}")
    driver.get("https://www.traveltodo.com/")
    print("Searching...")
    if search(driver, destination, check_in, check_out):
        print("Search success.")

        print("Scrapping...")
        hotels_info = extract_all_hotels_info(driver)

        retry = 0
        while len(hotels_info) == 0 and retry < 3:
            print("Retry attempt...")
            driver.refresh()
            hotels_info = extract_all_hotels_info(driver)
            retry += 1

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
            df.to_sql('traveltodo_src', engine, if_exists='append', index=False, chunksize=100000)
            print("Saved!")
        else:
            raise ValueError("No data returned, retrying...")
    else:
        raise ValueError("Search failed, retrying...")
