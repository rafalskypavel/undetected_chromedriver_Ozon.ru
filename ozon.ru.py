import logging
import os
import re
import threading
import time
import pandas as pd
from bs4 import BeautifulSoup
import undetected_chromedriver as uc
from contextlib import contextmanager


output_dir = os.path.join(os.getcwd(), 'output')
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Function to read URLs from file
def load_urls(filename):
    with open(filename, 'r') as file:
        return [line.strip() for line in file.readlines()]

# Function to read proxies from file
def load_proxies(filename):
    proxies = []
    with open(filename, 'r') as file:
        for line in file:
            host, port, user, passw = line.strip().split(',')
            proxies.append({"host": host, "port": port, "user": user, "pass": passw})
    return proxies

# Инициализация драйвера с прокси
def init_driver(proxy):
    logging.info(f"Initializing the driver with proxy settings: {proxy}")
    PROXY_HOST = proxy["host"]
    PROXY_PORT = proxy["port"]
    PROXY_USER = proxy["user"]
    PROXY_PASS = proxy["pass"]

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

def parse_product_card(product, logger, proxy):
    try:
        product_link = "https://ozon.ru" + product.find("a", class_="tile-hover-target")["href"]
        product_link = re.sub(r'\?.*$', '', product_link)  # Обрезаем всё, что идет после знака вопроса "?"
        product_title = product.find("span", class_="tsBody500Medium").text
        product_price = product.find("span", class_="tsHeadline500Medium").text
        # driver.get(product_link)
        # # Используем JavaScript для получения содержимого тега <script> напрямую
        # script_content = driver.execute_script("""
        #     var scriptTag = document.querySelector('script[type="application/ld+json"]');
        #     return scriptTag ? scriptTag.textContent : null;
        # """)
        #
        # if script_content:
        #     json_data = json.loads(script_content)
        #     # Распределяем данные по переменным
        #     offers = json_data.get('offers', {})
        #     product_name = json_data.get('name', 'Not Provided')
        #
        #     sku = json_data.get('sku', 'Not Provided')
        #     brand = json_data.get('brand', 'Not Provided')
        #     description = json_data.get('description', 'Not Provided')
        #     aggregate_rating = json_data.get('aggregateRating', {})
        #     availability = offers.get('availability', 'Not Provided')
        #     price = offers.get('price', 'Not Provided')
        #     price_currency = offers.get('priceCurrency', 'Not Provided')
        #     rating_value = aggregate_rating.get('ratingValue', 'Not Provided').replace('.', ',')
        #     review_count = aggregate_rating.get('reviewCount', 'Not Provided')
        # else:
        #     # Если тег <script> не найден, задаем значения по умолчанию
        #     product_type, offers, image_url, product_name, context, sku, brand, description, aggregate_rating = ['Not Provided'] * 9
        #     offer_url, availability, price, price_currency, rating_value, review_count = ['Not Provided'] * 6
        # logger.info(f"Parsed product: {product_name}, sku: {sku}, proxy: {proxy}")

        logger.info(f"{proxy}. Parsed product: {product_title}")

        # Возвращаем словарь с полной информацией о продукте
        return {
            'Ссылка': product_link,
            'Имя продукта': product_title,
            'Цена с Ozon картой': product_price,
        }

        # # Возвращаем словарь с полной информацией о продукте
        # return {
        #     'Ссылка': product_link,
        #     'SKU': sku,
        #     'Имя продукта': product_name,
        #     'Цена с Ozon картой': product_price,
        #     'Цена предложения': price,
        #     'Валюта цены': price_currency,
        #     'Бренд': brand,
        #     'Доступность': availability,
        #     'Рейтинг': rating_value,
        #     'Количество отзывов': review_count,
        #     'Описание': description
        # }
    except Exception as e:
        logging.error(f"Error parsing product card: {e}")
        return None

# def save_to_csv(data, file_name):
#     output_file = os.path.join(output_dir, file_name)  # Adjust path to use output directory
#     logging.info(f"Saving data to {output_file}")
#     df = pd.DataFrame(data)
#     df.to_csv(output_file, index=False, sep=";", encoding='utf-8-sig')

def setup_logger(name, log_file_name, level=logging.INFO, mode='a'):
    log_file = os.path.join(output_dir, log_file_name)
    handler = logging.FileHandler(log_file, mode=mode)
    handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger = logging.getLogger(name)
    if logger.hasHandlers():
        logger.handlers.clear()  # Очистка предыдущих обработчиков если они есть
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger


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


def scrape_and_save_to_csv(url, filename, proxy, logger):
    page_number = 200
    output_file = os.path.join(output_dir, filename)
    # Инициализация файла в режиме 'w' для очистки/создания нового файла.
    with open(output_file, mode='w', newline='', encoding='utf-8-sig') as f:
        pd.DataFrame(columns=['Ссылка', 'Имя продукта', 'Цена с Ozon картой']).to_csv(f, index=False, sep=";")

    try:
        with managed_driver(proxy) as driver:
            while True:
                full_url = f"{url}?page={page_number}"
                logger.info(f"Processing: {full_url}")
                driver.get(full_url)
                slow_scroll(driver, scroll_time=8, steps=20)

                soup = BeautifulSoup(driver.page_source, 'html.parser')
                container = soup.find("div", class_="widget-search-result-container")
                if not container or not container.find_all("div", class_="tile-root"):
                    logger.info("No more products found or end of pagination.")
                    break

                product_elements = container.find_all("div", class_="tile-root")
                for product in product_elements:
                    product_data = parse_product_card(product, logger, proxy)
                    if product_data:
                        # Добавление каждого продукта в файл в режиме 'a'
                        with open(output_file, mode='a', newline='', encoding='utf-8-sig') as f:
                            pd.DataFrame([product_data]).to_csv(f, header=False, index=False, sep=";")

                page_number += 1
    except Exception as e:
        logger.error(f"An error occurred: {e}")





def get_filename_from_url(url):
    # Extracts a meaningful part of the URL to use as the filename
    path = url.split('/')[-1]  # Gets the last part of the URL
    filename = f"ozon_products_{path}.csv"
    return filename

def thread_target(url, proxy, index):
    filename = get_filename_from_url(url)
    # Первая инициализация логгера с mode='w' для очистки предыдущих логов
    logger = setup_logger(f"logger_{index}", filename.replace('.csv', '.log'), mode='w')
    logger.info(f"Starting scraping for {url}")
    scrape_and_save_to_csv(url, filename, proxy, logger)

@contextmanager
def managed_driver(proxy):
    driver = None
    for _ in range(5):  # Try to initialize the driver up to 5 times
        try:
            driver = init_driver(proxy)
            if driver:
                break
        except Exception as e:
            logging.error(f"Driver initialization failed: {e}")
            time.sleep(30)
    if not driver:
        raise Exception("Failed to initialize the web driver after multiple attempts.")
    try:
        yield driver
    finally:
        driver.quit()


def main():
    urls = load_urls('BASE_URL.txt')
    proxies = load_proxies('proxies.txt')
    if len(urls) > len(proxies):
        logging.error("There are more URLs than proxies. Exiting.")
        return

    threads = []
    for index, url in enumerate(urls):
        proxy = proxies[index % len(proxies)]
        t = threading.Thread(target=thread_target, args=(url, proxy, index))
        threads.append(t)
        # Запуск первого потока без задержки, для остальных - с задержкой
        if index > 0:
            time.sleep(5)
        t.start()

    for t in threads:
        t.join()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    main()
