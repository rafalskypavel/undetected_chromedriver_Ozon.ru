import csv
import itertools
import json
import logging
import os
import tempfile
import threading
import time
from bs4 import BeautifulSoup
import undetected_chromedriver as uc
from contextlib import contextmanager
import selenium.webdriver.support.ui as ui
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from config import OUTPUT_DIR, CHROME_DRIVER_PATH, EXTENSION_DIR
from selenium.webdriver.chrome.service import Service
import pandas as pd

os.makedirs(EXTENSION_DIR, exist_ok=True)

def load_urls_from_txt(filename):
    with open(filename, 'r') as file:
        return [line.strip() for line in file.readlines()]

def load_urls_from_csv(filename):
    urls_dict = {"url": [], "fas": []}  # Используем списки для хранения URL и fas
    with open(filename, 'r', newline='', encoding='utf-8-sig') as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            if row[4]:
                urls_dict["url"].append(row[4])
            else:
                urls_dict["url"].append("None")
            if row[5]:
                urls_dict["fas"].append(row[5])
            else:
                urls_dict["fas"].append("None")
    return urls_dict

def load_proxies(filename):
    proxies = []
    with open(filename, 'r') as file:
        for line in file:
            host, port, user, passw = line.strip().split(',')
            proxies.append({"host": host, "port": port, "user": user, "pass": passw})
    return proxies

def init_driver(proxy, logger):
    logger.info(f"Initializing the driver with proxy settings: {proxy}")
    PROXY_HOST = proxy["host"]
    PROXY_PORT = proxy["port"]
    PROXY_USER = proxy["user"]
    PROXY_PASS = proxy["pass"]
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
    # Создаем временную директорию для профиля пользователя
    user_data_dir = tempfile.mkdtemp()
    chrome_options.add_argument(f"--user-data-dir={user_data_dir}")
    chrome_options.add_argument("--ignore-certificate-errors")
    # Отключение GPU: Может улучшить производительность в некоторых сценариях.
    chrome_options.add_argument("--disable-gpu")
    prefs = {"profile.managed_default_content_settings.images": 2}
    chrome_options.add_experimental_option("prefs", prefs)
    chrome_options.add_argument("--disable-popup-blocking")
    chrome_options.add_argument(f"--load-extension={PROXY_FOLDER}")
    service = Service(executable_path=CHROME_DRIVER_PATH)
    driver = uc.Chrome(headless=False, use_subprocess=False, options=chrome_options, service=service)
    return driver

def parse_product_card(url, fas, driver, soup, logger):
    product_info = {
        'Ссылка': url,
        'SKU': 'None',
        'Фасовка': fas,
        'Имя продукта': 'None',
        'Цена с Ozon картой': 'None',
        'Цена из json': 'None',
        'Валюта цены': 'None',
        'Бренд': 'None',
        'Доступность': 'None',
        'Рейтинг': 'None',
        'Количество отзывов': 'None'
    }
    try:
        script_content = driver.execute_script("""
            return document.querySelector('script[type="application/ld+json"]')?.textContent || null;
        """)
        ozon_card_price = soup.find("span", string="c Ozon Картой") \
            .find_previous("span") \
            .text if soup.find("span", string="c Ozon Картой") else "None"
        product_info['Цена с Ozon картой'] = ozon_card_price
        if script_content:
            json_data = json.loads(script_content)
            product_info.update({
                'SKU': json_data.get('sku', 'None'),
                'Имя продукта': json_data.get('name', 'None'),
                'Цена из json': json_data.get('offers', {}).get('price', 'None'),
                'Валюта цены': json_data.get('offers', {}).get('priceCurrency', 'None'),
                'Бренд': json_data.get('brand', 'None'),
                'Доступность': json_data.get('offers', {}).get('availability', 'None'),
                'Рейтинг': json_data.get('aggregateRating', {}).get('ratingValue', 'None').replace('.', ','),
                'Количество отзывов': json_data.get('aggregateRating', {}).get('reviewCount', 'None')
            })
        logger.info(f"Parsed product: {product_info['Имя продукта']}, sku: {product_info['SKU']}.")
    except Exception as e:
        logger.error(f"Error parsing product card: {e}")
        return None
    return product_info

def setup_logger(name, log_file_name, level=logging.INFO, mode='w'):
    if not OUTPUT_DIR.exists():
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    log_file = OUTPUT_DIR / log_file_name
    handler = logging.FileHandler(log_file,
                                  mode=mode)  # Path объекты нативно поддерживаются в FileHandler начиная с Python 3.6
    handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger

def slow_scroll(driver, scroll_time, steps):
    """
    Медленно прокручивает страницу вниз в течение указанного времени.

    Args:
    - driver: экземпляр драйвера Selenium.
    - scroll_time: общее время прокрутки в секундах.
    - steps: количество шагов прокрутки.
    """
    scroll_pause_time = scroll_time / steps
    last_height = driver.execute_script("return document.body.scrollHeight")
    for step in range(steps):
        new_height = (last_height / steps) * (step + 1)
        driver.execute_script(f"window.scrollTo(0, {new_height});")
        time.sleep(scroll_pause_time)

def scrape_and_save_to_csv(output_file, url, fas, driver, logger):  # Добавляем аргумент proxy
    try:
        logger.info(f"Processing {url}")
        original_window = driver.current_window_handle
        driver.switch_to.new_window('tab')
        driver.get(url)
        slow_scroll(driver, scroll_time=5, steps=10)
        wait = ui.WebDriverWait(driver, 12)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        product_data = parse_product_card(url, fas, driver, soup, logger)  # Передаем прокси в функцию парсинга
        if product_data:
            with open(output_file, mode='a', newline='', encoding='utf-8-sig') as f:
                pd.DataFrame([product_data]).to_csv(f, header=False, index=False, sep=";")
        driver.close()
        driver.switch_to.window(original_window)
    except Exception as e:
        logger.error(f"An error occurred: {e}")

@contextmanager
def managed_driver(proxy, logger):
    driver = None
    for _ in range(5):  # Try to initialize the driver up to 5 times
        try:
            driver = init_driver(proxy, logger)
            time.sleep(5)
            if driver:
                break
        except Exception as e:
            logger.error(f"Driver initialization failed: {e}")
            time.sleep(10)
    if not driver:
        raise Exception("Failed to initialize the web driver after multiple attempts.")
    try:
        yield driver
    finally:
        driver.quit()
        time.sleep(5)
        driver.quit()

def chunks(lst, n):
    """Разбивает список на куски по `n` элементов."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def main():
    urls_dict = load_urls_from_csv(r'маркетплейсы.csv')  # Используем функцию для загрузки в словарь
    proxies = load_proxies('proxies.txt')
    logger = setup_logger('my_logger', 'ozon_ru_logger.log')
    if len(urls_dict) == 0 or len(proxies) == 0:  # Проверяем наличие данных в словаре, а не в списке
        logger.error("No URLs or proxies found. Exiting.")
        return
    filename = 'ozon_products.csv'
    output_file = os.path.join(OUTPUT_DIR, filename)
    with open(output_file, mode='w', newline='', encoding='utf-8-sig') as f:
        pd.DataFrame(columns=[
            'Ссылка',
            'SKU',
            'Фасовка',
            'Имя продукта',
            'Цена с Ozon картой',
            'Цена из json',
            'Валюта цены',
            'Бренд',
            'Доступность',
            'Рейтинг',
            'Количество отзывов'
        ]).to_csv(f, index=False, sep=";")
    threads = []
    proxy_cycle = itertools.cycle(proxies)
    start_total_time = time.time()
    for url_chunk in chunks(urls_dict["url"], 100):
        proxy = next(proxy_cycle)
        # Передаем весь urls_dict в поток, а не только часть url и fas
        t = threading.Thread(target=thread_target, args=(output_file, url_chunk, urls_dict, proxy, logger))
        threads.append(t)
        t.start()
        time.sleep(5)
    for t in threads:
        t.join()
    end_total_time = time.time()
    logger.info(f"Total execution time: {end_total_time - start_total_time} seconds")

def thread_target(output_file, url_chunk, urls_dict, proxy, logger):
    start_time = time.time()
    try:
        with managed_driver(proxy, logger) as driver:
            for url in url_chunk:
                # Находим индекс URL в исходном списке urls_dict["url"]
                index = urls_dict["url"].index(url)
                # Получаем соответствующее значение fas, используя этот индекс
                fas = urls_dict["fas"][index]
                time.sleep(2)
                scrape_and_save_to_csv(output_file, url, fas, driver, logger)
    except Exception as e:
        logger.error(f"An error occurred in thread: {e}")
    end_time = time.time()
    logger.info(f"Thread execution time: {end_time - start_time} seconds")




if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    main()
