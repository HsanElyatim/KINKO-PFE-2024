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
    """
        Selects a destination from an autocomplete dropdown in a search form.

        Parameters:
            driver: selenium.webdriver instance controlling the browser.
            search_form: WebElement representing the search form.
            destination: str, the destination to be selected from the autocomplete suggestions.

        Returns:
            None
    """
    try:
        # Locate the destination input field and enter the destination
        destination_input = search_form.find_element(By.CSS_SELECTOR, "input[data-selenium='textInput']")
        destination_input.send_keys(destination)

        # Wait for the autocomplete panel to be visible
        autocomplete_panel = WebDriverWait(search_form, 10).until(EC.visibility_of_element_located(
            (By.CSS_SELECTOR, "div[data-selenium='autocompletePanel']")
        ))

        # Find all autocomplete items and click the one matching the destination
        autosuggest_items = autocomplete_panel.find_elements(By.CSS_SELECTOR,
                                                             "li[data-selenium='autosuggest-item']")
        for autosuggest_item in autosuggest_items:
            if autosuggest_item.get_attribute("data-text") == destination:
                driver.execute_script("arguments[0].click();", autosuggest_item)
                break
    except (NoSuchElementException, TimeoutException, ElementClickInterceptedException) as e:
        print("ERROR selecting destination!")


def select_month(driver, range_picker, month):
    """
        Selects a specific month in a date range picker.

        Parameters:
            driver: selenium.webdriver instance controlling the browser.
            range_picker: WebElement representing the date range picker.
            month: str, the target month in the format "Month Year" (e.g., "June 2024").

        Returns:
            WebElement representing the left calendar of the selected month.
    """
    try:
        # Locate the left calendar within the date range picker
        left_calendar = range_picker.find_element(By.CLASS_NAME, "DayPicker-Month")

        # Loop until the current month matches the target month
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
            # Update the left calendar element
            left_calendar = range_picker.find_element(By.CLASS_NAME, "DayPicker-Month")

        # Return the left calendar element of the selected month
        return left_calendar
    except (NoSuchElementException, ElementClickInterceptedException):
        print("ERROR selecting month!")


def select_date(driver, range_picker, day, month):
    """
        Selects a specific date in a date range picker.

        Parameters:
            driver: selenium.webdriver instance controlling the browser.
            range_picker: WebElement representing the date range picker.
            day: str, the day to be selected (e.g., "15").
            month: str, the target month in the format "Month Year" (e.g., "June 2024").

        Returns:
            None
    """
    try:
        # Select the specific month first
        calendar = select_month(driver, range_picker, month)

        # Locate all days within the calendar
        days_picker = calendar.find_elements(By.CLASS_NAME, "PriceSurgePicker-Day")

        # Iterate through days and select the matching day
        for day_picker in days_picker:
            if day_picker.text == day:
                driver.execute_script("arguments[0].click();", day_picker)
                break
    except (NoSuchElementException, ElementClickInterceptedException) as e:
        # print(e)
        print("ERROR selecting date!")


def select_checkin_checkout(driver, search_form, check_in, check_out):
    """
        Selects check-in and check-out dates in a date range picker.

        Parameters:
            driver: selenium.webdriver instance controlling the browser.
            search_form: WebElement representing the search form containing the date range picker.
            check_in: datetime object representing the check-in date.
            check_out: datetime object representing the check-out date.

        Returns:
            None
    """
    check_in_month, check_in_day = check_in.strftime("%B") + " " + str(check_in.year), str(check_in.day)
    check_out_month, check_out_day = check_out.strftime("%B") + " " + str(check_out.year), str(check_out.day)

    try:
        # Locate the range picker within the search form
        range_picker = WebDriverWait(search_form, 10).until(EC.visibility_of_element_located(
            (By.CSS_SELECTOR, "div[data-selenium='rangePickerCheckIn']")
        ))

        # Select the check-in date
        select_date(driver, range_picker, check_in_day, check_in_month)

        # Select the check-out date
        select_date(driver, range_picker, check_out_day, check_out_month)
    except TimeoutException:
        print("ERROR selecting check-in check-out!")


def search(driver, destination, check_in, check_out):
    """
        Performs a search operation by selecting destination, check-in, and check-out dates.

        Parameters:
            driver: selenium.webdriver instance controlling the browser.
            destination: str, the destination to search for.
            check_in: datetime object representing the check-in date.
            check_out: datetime object representing the check-out date.

        Returns:
            bool: True if the search was successful, False otherwise.
    """
    try:
        # Locate the search form
        search_form = driver.find_element(By.CSS_SELECTOR, "div[data-selenium='searchBox']")

        # Select the destination
        select_destination(driver, search_form, destination)

        # Select check-in and check-out dates
        select_checkin_checkout(driver, search_form, check_in, check_out)

        # Click the search button
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
    """
        Retrieves the link to the hotel from a hotel element on the search results page.

        Parameters:
            driver: selenium.webdriver instance controlling the browser.
            hotel_elm: WebElement representing the hotel element.

        Returns:
            str: The URL link to the hotel, or None if an error occurs.
    """
    try:
        # Scroll the hotel element into view
        driver.execute_script("arguments[0].scrollIntoView(true);", hotel_elm)

        # Return the hotel link
        return hotel_elm.find_element(By.TAG_NAME, "a").get_attribute("href")
    except (NoSuchElementException, StaleElementReferenceException) as e:
        print("ERROR getting hotel link!")


def get_room_offer_info(room_offer):
    """
        Retrieves information about a room offer from a hotel booking website.

        Parameters:
            room_offer: WebElement representing the room offer.

        Returns:
            dict: A dictionary containing the room offer's benefits, capacity, price, and availability.
                  Returns None if an error occurs.
    """
    try:
        # Locate and extract benefits of the room offer
        benefits_container = room_offer.find_element(By.CLASS_NAME, "ChildRoomsList-roomCell-featureBuckets")
        benefits = benefits_container.find_elements(By.CSS_SELECTOR, "div[data-selenium='ChildRoomList-roomFeature']")

        # Calculate the room capacity based on the number of icons
        capacity = len(room_offer.find_element(By.CLASS_NAME, "ChildRoomsList-roomCell-capacity")
                       .find_elements(By.TAG_NAME, "i"))

        # Extract the room price
        price = room_offer.find_element(By.CSS_SELECTOR, "span[data-selenium='PriceDisplay']").text

        # Extract the room availability information
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
    """
        Retrieves detailed information about a room from a hotel booking website.

        Parameters:
            room_elm: WebElement representing the room element.

        Returns:
            list: A list of dictionaries, each containing information about a room offer.
                  Returns an empty list if no room offers are found or if an error occurs.
    """
    try:
        # Extract the room type
        room_type = room_elm.find_element(By.CSS_SELECTOR, "span[data-selenium='masterroom-title-name']").text

        try:
            # Locate the container for room offers
            room_offers_container = room_elm.find_element(By.CLASS_NAME, "MasterRoom-roomsList") \
                .find_element(By.CLASS_NAME, "ChildRoomsList")
        except NoSuchElementException as e:
            print("No rooms found!")
            return []

        # Find all room offers
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
    """
        Retrieves detailed information about a hotel from a hotel booking website.

        Parameters:
            driver: WebDriver instance used to navigate the hotel booking website.

        Returns:
            list: A list of dictionaries, each containing information about a room offer for the hotel.
                  Returns an empty list if no room offers are found or if an error occurs.
    """
    try:
        # Wait for the property content to be present
        property_content = WebDriverWait(driver, 10) \
            .until(EC.presence_of_element_located((By.ID, "property-main-content")))

        # Extract the hotel name and star rating
        hotel_name = property_content.find_element(By.CSS_SELECTOR, "p[data-selenium='hotel-header-name']").text
        hotel_stars = len(property_content.find_element(By.CLASS_NAME, "star-rating")
                          .find_elements(By.TAG_NAME, "svg"))

        try:
            # Wait for the room grid to be present
            room_grid = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "roomGrid")))
        except TimeoutException as e:
            driver.refresh()
            room_grid = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "roomGrid")))

        # Find all room elements
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
    """
        Loads more hotels into the list view by scrolling and fetching hotel items.

        Parameters:
            driver: WebDriver instance used to interact with the web page.

        Returns:
            list: A list of WebElement objects representing the loaded hotel items.
                  Returns an empty list if no hotel items are found or if an error occurs.
    """
    try:
        sleep(5)  # Pause to ensure content loads
        hotels_list_containers = driver.find_elements(By.CLASS_NAME, 'hotel-list-container')

        # Assuming the second container holds the hotel items, adjust index if necessary
        hotels_list = hotels_list_containers[1].find_elements(By.CSS_SELECTOR, "li[data-selenium='hotel-item']")

        driver.execute_script("arguments[0].scrollIntoView(true);",
                              driver.find_element(By.CLASS_NAME, "Footer"))

        return hotels_list
    except NoSuchElementException as e:
        print("ERROR loading more to hotels list!")


def get_hotels_list(driver):
    """
        Retrieves a list of hotel links from the hotel list view on a booking website.

        Parameters:
            driver: WebDriver instance used to interact with the web page.

        Returns:
            list: A list of strings representing URLs to each hotel's details page.
                  Returns an empty list if no hotels are found or if an error occurs.
    """
    try:
        sleep(5)  # Pause to ensure content loads
        hotel_filter = driver.find_element(By.CSS_SELECTOR,
                                           "span[data-component='search-filter-accommodationtype'][aria-label='Hotel']")
        driver.execute_script("arguments[0].click();", hotel_filter)

        # Wait for the hotel list container to be visible
        hotels_list_container = WebDriverWait(driver, 10) \
            .until(EC.visibility_of_element_located((By.CLASS_NAME, 'hotel-list-container')))

        # Wait for all hotel items to be present in the list
        hotels_list = WebDriverWait(hotels_list_container, 10) \
            .until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "li[data-selenium='hotel-item']")))

        # Extract links for each hotel item
        hotel_links = [get_hotel_link(driver, hotel_elm) for hotel_elm in hotels_list]

        # Scroll to the last hotel item and footer to load more hotels
        driver.execute_script("arguments[0].scrollIntoView(true);", hotels_list[-1])
        sleep(2)  # Pause for scrolling
        driver.execute_script("arguments[0].scrollIntoView(true);",
                              driver.find_element(By.CLASS_NAME, "Footer"))

        sleep(10)  # Pause to load more content

        # Fetch additional hotels from the second container if available
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
    """
        Extracts information for all hotels from the list of hotel links.

        Parameters:
            driver: WebDriver instance used to interact with the web page.

        Returns:
            list: A list of dictionaries, each containing hotel information.
                  Returns an empty list if no hotels are found or if an error occurs.
    """
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
    """
        Executes the entire scraping process for Agoda, including search, extraction,
        and saving the scraped data into a PostgreSQL database.

        Parameters:
            destination (str): The destination city or location to search for hotels.
            check_in (datetime.date): The check-in date.
            check_out (datetime.date): The check-out date.

        Returns:
            None
    """
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
            df['destination'] = destination
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
