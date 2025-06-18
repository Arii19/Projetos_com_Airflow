import json
import pendulum
from datetime import datetime
from airflow.decorators import dag, task
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import time

urls = {
    "Amazon": "https://www.amazon.com.br/Apple-iPhone-16-Plus-128/dp/B0DGMPCP48",
    "CasasBahia": "https://www.casasbahia.com.br/apple-iphone-16-128gb-branco/p/55067622",
    "Magalu": "https://www.magazineluiza.com.br/iphone-16-128gb-5g-branco-tela-61-camera-48mp-apple/p/ddd5a7h697/te/teip/",
    "Americanas": "https://www.americanas.com.br/produto/55062209"
}

def iniciar_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    driver = webdriver.Chrome(options=chrome_options)
    return driver

def extrair_preco_amazon(driver, url):
    driver.get(url)
    time.sleep(5)
    try:
        preco = driver.find_element(By.CLASS_NAME, "a-price-whole").text
        return float(preco.replace(".", "").replace(",", "."))
    except:
        return None

def extrair_preco_casasbahia(driver, url):
    driver.get(url)
    time.sleep(5)
    try:
        preco = driver.find_element(By.CSS_SELECTOR, '[data-testid="price-value"]').text
        return float(preco.replace("R$", "").replace(".", "").replace(",", "."))
    except:
        return None

def extrair_preco_magalu(driver, url):
    driver.get(url)
    time.sleep(5)
    try:
        preco = driver.find_element(By.CLASS_NAME, "sc-d79c9c3f-0").text
        return float(preco.replace("R$", "").replace(".", "").replace(",", "."))
    except:
        return None

def extrair_preco_americanas(driver, url):
    driver.get(url)
    time.sleep(5)
    try:
        preco = driver.find_element(By.CLASS_NAME, "src__BestPrice-sc-1jvw02c-5").text
        return float(preco.replace("R$", "").replace(".", "").replace(",", "."))
    except:
        return None

@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2024, 6, 1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["iphone", "monitoramento", "preco"]
)
def extrai_menor_preco():

    @task()
    def coletar_precos():
        driver = iniciar_driver()
        precos = {
            "Amazon": extrair_preco_amazon(driver, urls["Amazon"]),
            "CasasBahia": extrair_preco_casasbahia(driver, urls["CasasBahia"]),
            "Magalu": extrair_preco_magalu(driver, urls["Magalu"]),
            "Americanas": extrair_preco_americanas(driver, urls["Americanas"]),
        }
        driver.quit()
        return precos

    @task()
    def salvar_precos(precos: dict):
        precos["data_coleta"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open("/opt/airflow/dags/precos_iphone.json", "a") as f:
            f.write(json.dumps(precos) + "\n")

    salvar_precos(coletar_precos())

dag = extrai_menor_preco()
