from selenium import webdriver
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.firefox.options import Options


def init_firefox_driver():
    """
        Initializes a headless Firefox WebDriver with specific options for scraping.

        Returns:
            WebDriver: Initialized Firefox WebDriver instance.
    """
    # Set up Firefox options
    firefox_options = Options()
    firefox_options.add_argument("--headless")  # Run in headless mode
    firefox_options.add_argument("--no-sandbox")  # Required for running as root user
    firefox_options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource problems

    # Initialize the WebDriver for Firefox
    geckodriver_path = "/usr/local/bin/geckodriver"
    driver = webdriver.Firefox(service=FirefoxService(geckodriver_path), options=firefox_options)
    return driver
