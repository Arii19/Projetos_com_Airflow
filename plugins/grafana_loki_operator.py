import json
import requests
import pendulum
from airflow.models.baseoperator import BaseOperator

class GrafanaLokiOperator(BaseOperator):
    def __init__(self, name: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.name = name

    def SendLog(line: str):
        payload = {
            "streams": [
                {
                    "stream": {"Source": "SmartFlow"},
                    "values": [[str(int(pendulum.now("America/Sao_Paulo").timestamp() * 1e9)), line]]
                }
            ]
        }
        headers = {
            "Content-Type": "application/json"
        }
        response = requests.post("https://logs-prod-024.grafana.net/loki/api/v1/push", data=json.dumps(payload), headers=headers)
        if response.status_code != 204:
            print(f"Erro ao enviar log para o Loki: {response.status_code} - {response.text}")

