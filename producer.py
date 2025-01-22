from datetime import datetime
import json
import requests
import time
import os
from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.layout import Layout
from kafka import KafkaProducer

console = Console()
layout = Layout()

# Configuración de Kafka
KAFKA_TOPIC = 'ibm_options'
KAFKA_SERVER = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def fetch_batch_data(symbol, apikey, batch_size=1000, function="HISTORICAL_OPTIONS"):
    """Obtiene datos históricos por lotes desde la API de Alpha Vantage"""
    base_url = 'https://www.alphavantage.co/query?function=HISTORICAL_OPTIONS&symbol=IBM&date=2017-11-15&apikey=demo'
    all_data = []
    for start in range(0, batch_size, 100):
        params = {
            'function': function,
            'symbol': symbol,
            'apikey': apikey,
            'datatype': 'json',
            'outputsize': 'full'
        }
        response = requests.get(base_url, params=params)
        
        if response.status_code != 200:
            console.print(f"[red]Error en la solicitud: {response.status_code} {response.reason}[/red]")
            continue
        
        data = response.json()
        
        if 'data' in data:
            all_data.extend(data['data'])
        else:
            console.print(f"[red]Error en la respuesta de datos: {data.get('Error Message', response.text)}[/red]")

    return all_data

def main():
    symbol = 'IBM'
    apikey = os.getenv('ALPHA_VANTAGE_API_KEY', '1PZJ2AMWREX60DI1')  
    last_time = time.time()

    while True:
        current_time = time.time()
        
        data_batch = fetch_batch_data(symbol, apikey, batch_size=100)
        
        data_with_meta = {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'entries_count': len(data_batch),
            'data': data_batch
        }

        producer.send(KAFKA_TOPIC, value=data_with_meta)
        console.print(f"[blue]Datos enviados al topic {KAFKA_TOPIC}: {len(data_with_meta['data'])} entradas[/blue]")
        
        time.sleep(60)  

if __name__ == "__main__":
    console.print("[yellow]Starting IBM Options Data Producer...[/yellow]")
    try:
        main()
    except KeyboardInterrupt:
        console.print("[red]Producer stopped by user[/red]")