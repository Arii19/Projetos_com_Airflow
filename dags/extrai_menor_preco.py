import requests
from bs4 import BeautifulSoup
from airflow.decorators import dag, task
import pendulum

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

urls = {
    "Amazon": "https://www.amazon.com.br/Apple-iPhone-16-Plus-128/dp/B0DGMPCP48",
    "CasasBahia": "https://www.casasbahia.com.br/apple-iphone-16-128gb-branco/p/55067622",
    "Magalu": "https://www.magazineluiza.com.br/iphone-16-128gb-5g-branco-tela-61-camera-48mp-apple/p/ddd5a7h697/te/teip/",
    "Americanas": "https://www.americanas.com.br/produto/55062209"
}

# Funções de scraping (devem vir antes da DAG)

def extrair_amazon(url):
    try:
        r = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(r.text, "lxml")
        preco = soup.find("span", class_="a-price-whole")
        return float(preco.text.strip().replace(".", "").replace(",", "."))
    except:
        return None

def extrair_casas_bahia(url):
    try:
        r = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(r.text, "lxml")
        preco = soup.find("strong", {"data-testid": "price-value"})
        return float(preco.text.strip().replace("R$", "").replace(".", "").replace(",", "."))
    except:
        return None

def extrair_magalu(url):
    try:
        r = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(r.text, "lxml")
        preco = soup.find("p", class_="sc-d79c9c3f-0 kAZgZy")  # classe pode mudar
        return float(preco.text.strip().replace("R$", "").replace(".", "").replace(",", "."))
    except:
        return None

def extrair_americanas(url):
    try:
        r = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(r.text, "lxml")
        preco = soup.find("span", class_="src__BestPrice-sc-1jvw02c-5")
        return float(preco.text.strip().replace("R$", "").replace(".", "").replace(",", "."))
    except:
        return None

# DAG
@dag(
    schedule='@daily',
    start_date=pendulum.datetime(2024, 6, 1, tz="UTC"),
    catchup=False,
)
def extrai_menor_preco():
    @task
    def extrai_precos():
        precos = {}
        for loja, url in urls.items():
            if loja == "Amazon":
                precos["Amazon"] = extrair_amazon(url)
            elif loja == "CasasBahia":
                precos["CasasBahia"] = extrair_casas_bahia(url)
            elif loja == "Magalu":
                precos["Magalu"] = extrair_magalu(url)
            elif loja == "Americanas":
                precos["Americanas"] = extrair_americanas(url)
        return precos

    @task
    def salva_precos(precos):
        import json
        from datetime import datetime
        data_atual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        precos["data_coleta"] = data_atual
        with open("/opt/airflow/dados/precos_iphone.json", "a") as f:
            f.write(json.dumps(precos) + "\n")

    precos = extrai_precos()
    salva_precos(precos)

dag = extrai_menor_preco()
