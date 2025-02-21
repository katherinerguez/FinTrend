from datetime import datetime
import json
import requests
import time
import os
from rich.console import Console
from kafka import KafkaProducer
from kafka.errors import KafkaError

console = Console()

# Configuración de Kafka
KAFKA_TOPIC = 'ibm_options'
KAFKA_SERVER = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def fetch_batch_data(symbol, apikey, batch_size=1000, function="HISTORICAL_OPTIONS", max_retries=3, delay=5):
    """Obtiene datos históricos por lotes desde la API de Alpha Vantage con manejo de errores y reintentos."""
    base_url = 'https://www.alphavantage.co/query'
    all_data = []

    for start in range(0, batch_size, 100):
        params = {
            'function': function,
            'symbol': symbol,
            'apikey': apikey,
            'datatype': 'json',
            'outputsize': 'full'
        }

        for attempt in range(max_retries):
            try:
                response = requests.get(base_url, params=params)

                if response.status_code == 200:
                    data = response.json()
                    
                    # Verificar si la respuesta contiene datos válidos
                    if 'data' in data:
                        all_data.extend(data['data'])
                        break  
                    else:
                        console.print(f"[red]Error en la respuesta de datos: {data.get('Error Message', response.text)}[/red]")
                        break  
                else:
                    console.print(f"[red]Error en la solicitud: {response.status_code} {response.reason}[/red]")
            
            except requests.exceptions.RequestException as e:
                console.print(f"[red]Error de conexión: {e}[/red]")
          
            if attempt < max_retries - 1:
                console.print(f"[yellow]Reintentando en {delay} segundos... (Intento {attempt + 1}/{max_retries})[/yellow]")
                time.sleep(delay)
            else:
                console.print(f"[red]Fallo después de {max_retries} intentos.[/red]")
                break  

    return all_data

def main():
    symbol = 'IBM'
    apikey = os.getenv('ALPHA_VANTAGE_API_KEY', '1PZJ2AMWREX60DI1')  
    
    while True:
        try:
            data_batch = fetch_batch_data(symbol, apikey, batch_size=100)
            
            data_with_meta = {
                'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'entries_count': len(data_batch),
                'data': data_batch
            }

            # Enviar datos a Kafka
            future = producer.send(KAFKA_TOPIC, value=data_with_meta)
            future.add_callback(lambda x: console.print(f"[green]Datos enviados al topic {KAFKA_TOPIC}.[/green]"))
            future.add_errback(lambda e: console.print(f"[red]Error al enviar datos a Kafka: {e}[/red]"))
            
            console.print(f"[blue]Datos preparados para enviar: {len(data_with_meta['data'])} entradas[/blue]")
        
        except Exception as e:
            console.print(f"[red]Error en el proceso principal: {e}[/red]")
        
        time.sleep(60)  

if __name__ == "__main__":
    console.print("[yellow]Starting IBM Options Data Producer...[/yellow]")
    try:
        main()
    except KeyboardInterrupt:
        console.print("[red]Producer stopped by user[/red]")
    finally:
        producer.close()  