import pendulum
import requests
from bs4 import BeautifulSoup
from airflow.decorators import dag, task
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.macros import ds_add
from os.path import join
import pandas as pd
import os

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

urls = {
    "Amazon": "https://www.amazon.com.br/Apple-iPhone-16-Plus-128/dp/B0DGMPCP48/ref=sr_1_1_sspa?__mk_pt_BR=%C3%85M%C3%85%C5%BD%C3%95%C3%91&crid=2SI0HXNAIXBID&dib=eyJ2IjoiMSJ9.mKL0CDUL-izWEwZ_TrTpr6w8TMYo7WxevYEgLmU-FFa1Jc77GmzYKxvLfFP-MGP9Uq_eiByprCq8sL8E8RC_4CX2H-S6pNwnJfXMEviY3RWB5lt0auECPzLf4sezMCYIvrJyI5wKcLKFCfZbvxjGWoatJjLqp4UByhnwSA8J8MjtIr8UlRrclltnfYg8JFFvrKNr3tHFDKtj1x_Sh2uSohMQYabyS5H5YnbDT-LhQUKe0mPNhqUURh3-kLZNxKiK5HZcKTICLhyr55wafexlgzZn2Jkrx_zUVnGulzk7JiU.Hyf-Ast-xTjHAn9kodtcfdYBOVDdM55TFWRhbVMrXtY&dib_tag=se&keywords=iphone%2B16&qid=1750269162&sprefix=iphone16%2Caps%2C191&sr=8-1-spons&ufe=app_do%3Aamzn1.fos.9e6a115c-05b9-4b96-8e1c-b1f9ce2ac1a6&sp_csd=d2lkZ2V0TmFtZT1zcF9hdGY&th=1",
    "CasasBahia": "https://www.casasbahia.com.br/apple-iphone-16-128gb-branco/p/55067622?utm_source=Google&utm_medium=BuscaOrganica&utm_campaign=DescontoEspecial",
    "Magalu": "https://www.magazineluiza.com.br/iphone-16-128gb-5g-branco-tela-61-camera-48mp-apple/p/ddd5a7h697/te/teip/",
    "Americanas": "https://www.americanas.com.br/produto/55062209"
}

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
        preco = soup.find("p", class_="sc-d79c9c3f-0 kAZgZy")  # classe pode mudar!
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

@dag(
    schedule='@daily',
    start_date=pendulum.date_
)
def main_dag():

    @task()
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

    @task()
    def salva_precos(precos):
        import json
        from datetime import datetime
        data_atual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        precos["data_coleta"] = data_atual
        with open("/path/to/seu_arquivo.json", "a") as f:
            f.write(json.dumps(precos) + "\n")

    precos = extrai_precos()
    salva_precos(precos)

main_dag()