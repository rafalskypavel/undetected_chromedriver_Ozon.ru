import logging
import os
import time
import pandas as pd
from bs4 import BeautifulSoup
from selenium.common.exceptions import NoSuchElementException
import undetected_chromedriver as uc
from contextlib import contextmanager

BASE_URL = "https://ozon.ru/category/gvozdi-9771/"
PAGE_WAIT_TIMEOUT = 10

logging.basicConfig(filename='parser.log', filemode='w', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Инициализация драйвера с прокси
def init_driver():
    logging.info("Initializing the driver")
    PROXY_HOST = "194.242.124.134"
    PROXY_PORT = "8000"
    PROXY_USER = "n9HucD"
    PROXY_PASS = "MPyRo4"

    # Создание директории для расширения, если она ещё не существует
    PROXY_FOLDER = os.path.join(os.getcwd(), 'extension', 'proxy_folder')
    os.makedirs(PROXY_FOLDER, exist_ok=True)

    manifest_json = """
    {
        "version": "1.0.0",
        "manifest_version": 2,
        "name": "Chrome Proxy",
        "permissions": [
            "proxy",
            "tabs",
            "unlimitedStorage",
            "storage",
            "<all_urls>",
            "webRequest",
            "webRequestBlocking"
        ],
        "background": {
            "scripts": ["background.js"]
        },
        "minimum_chrome_version":"22.0.0"
    }
    """

    background_js = '''
    var config = {
            mode: "fixed_servers",
            rules: {
            singleProxy: {
                scheme: "http",
                host: "%s",
                port: parseInt(%s)
            },
            bypassList: ["localhost"]
            }
        };

    chrome.proxy.settings.set({value: config, scope: "regular"}, function() {});

    function callbackFn(details) {
        return {
            authCredentials: {
                username: "%s",
                password: "%s"
            }
        };
    }

    chrome.webRequest.onAuthRequired.addListener(
                callbackFn,
                {urls: ["<all_urls>"]},
                ['blocking']
    );
    ''' % (PROXY_HOST, PROXY_PORT, PROXY_USER, PROXY_PASS)

    with open(os.path.join(PROXY_FOLDER, "manifest.json"), "w") as f:
        f.write(manifest_json)
    with open(os.path.join(PROXY_FOLDER, "background.js"), "w") as f:
        f.write(background_js)

    chrome_options = uc.ChromeOptions()
    # Мобильный эмулятор: Эмуляция мобильного устройства.
    # mobile_emulation = {"deviceName": "iPhone X"}
    # chrome_options.add_experimental_option("mobileEmulation", mobile_emulation)
    # Включение логирования драйвера: Помогает в отладке, позволяя просматривать логи драйвера.
    # chrome_options.add_argument("--enable-logging")
    # chrome_options.add_argument("--v=1")

    chrome_options.add_argument("--ignore-certificate-errors")

    # Отключение GPU: Может улучшить производительность в некоторых сценариях.
    chrome_options.add_argument("--disable-gpu")

    # Отключение изображений: Ускорение загрузки страниц за счет отключения загрузки изображений.
    prefs = {"profile.managed_default_content_settings.images": 2}
    chrome_options.add_experimental_option("prefs", prefs)

    # Отключение всплывающих окон: Блокировка всплывающих окон для улучшения взаимодействия с страницей.
    chrome_options.add_argument("--disable-popup-blocking")

    chrome_options.add_argument(f"--load-extension={PROXY_FOLDER}")

    # use_subprocess=True может быть полезным при работе в многопоточных или многопроцессных средах,
    # так как это позволяет изолировать каждый экземпляр драйвера в своем собственном процессе, минимизируя взаимное влияние и конфликты.
    driver = uc.Chrome(headless=False, use_subprocess=False, options=chrome_options)
    return driver

def close_driver(driver):
    logging.info("Closing the driver")
    time.sleep(5)
    driver.close()
    driver.quit()

def parse_product_card(product):
    try:
        product_link = "https://ozon.ru" + product.find("a", class_="tile-hover-target")["href"]
        product_title = product.find("span", class_="tsBody500Medium").text
        product_price = product.find("span", class_="tsHeadline500Medium").text
        logging.info(f"Parsed product: {product_title}")
        return {'Название': product_title, 'Цена': product_price, 'Ссылка': product_link}
    except Exception as e:
        logging.error(f"Error parsing product card: {e}")
        return None

def save_to_csv(data, file_name):
    logging.info(f"Saving data to {file_name}")
    df = pd.DataFrame(data)
    df.to_csv(file_name, index=False, sep=";", encoding='utf-8-sig')


def slow_scroll(driver, scroll_time=8, steps=20):
    """
    Медленно прокручивает страницу вниз в течение указанного времени.

    Args:
    - driver: экземпляр драйвера Selenium.
    - scroll_time: общее время прокрутки в секундах.
    - steps: количество шагов прокрутки.
    """
    scroll_pause_time = scroll_time / steps
    # Получаем высоту страницы
    last_height = driver.execute_script("return document.body.scrollHeight")

    for step in range(steps):
        # Вычисляем насколько прокручивать за один шаг
        new_height = (last_height / steps) * (step + 1)
        # Прокручиваем страницу на вычисленную высоту
        driver.execute_script(f"window.scrollTo(0, {new_height});")
        # Ждем загрузки страницы
        time.sleep(scroll_pause_time)


def scrape_and_save_to_csv(url):
    all_product_data = []
    page_number = 1
    with managed_driver() as driver:
        try:
            while True:
                logging.info(f"Processing page: {page_number}")
                driver.get(f"{url}?page={page_number}")

                # Прокручиваем страницу медленно
                slow_scroll(driver, scroll_time=8, steps=20)

                soup = BeautifulSoup(driver.page_source, 'html.parser')
                container = soup.find("div", class_="widget-search-result-container")
                if container is None:
                    logging.info("No more products found. Exiting.")
                    break

                product_elements = container.find_all("div", class_="tile-root")
                for product in product_elements:
                    product_data = parse_product_card(product)
                    if product_data:
                        all_product_data.append(product_data)
                page_number += 1
        except NoSuchElementException as e:
            logging.error(f"Exception caught: {e}")
        finally:
            if all_product_data:
                save_to_csv(all_product_data, 'ozon_products.csv')


@contextmanager
def managed_driver():
    driver = init_driver()
    try:
        yield driver
    finally:
        close_driver(driver)

if __name__ == "__main__":
    logging.info(f"Starting web scraping for URL: {BASE_URL}")
    scrape_and_save_to_csv(BASE_URL)
    logging.info("Web scraping completed")
