from datetime import datetime
import json
import os
import time
from rich.console import Console
from rich.live import Live
from rich.table import Table
from hdfs import InsecureClient
from kafka import KafkaConsumer

console = Console()

# Configuración de HDFS
HDFS_URL = 'http://172.18.0.4:9870'
HDFS_DIR = '/user/data/ibm_options/'


def setup_hdfs():
    """Configura la conexión HDFS y crea el directorio si no existe"""
    try:
        client = InsecureClient(HDFS_URL, user='root')
        try:
            client.status(HDFS_DIR)
            console.print(f"[green]Directorio HDFS {HDFS_DIR} existe[/green]")
        except:
            client.makedirs(HDFS_DIR)
            console.print(f"[yellow]Directorio HDFS {HDFS_DIR} creado[/yellow]")

        return client
    except Exception as e:
        console.print(f"[red]Error de conexión con HDFS: {str(e)}[/red]")
        return None


# Configuración local
LOCAL_DIR = './ibm_options_data/'
os.makedirs(LOCAL_DIR, exist_ok=True)

# Configurar HDFS
hdfs_client = setup_hdfs()


def save_data(data, timestamp):
    """Guarda los datos en HDFS o localmente"""
    filename = f"ibm_options_{timestamp}.json"

    if hdfs_client:
        try:
            file_path = f"{HDFS_DIR}{filename}"
            with hdfs_client.write(file_path, encoding='utf-8') as writer:
                json.dump(data, writer)
            console.print(f"[green]Archivo guardado en HDFS: {filename}[/green]")
            return "HDFS", filename
        except Exception as e:
            console.print(f"[red]Error al escribir en HDFS: {str(e)}[/red]")

    local_path = os.path.join(LOCAL_DIR, filename)
    with open(local_path, 'w', encoding='utf-8') as f:
        json.dump(data, f)
    console.print(f"[yellow]Archivo guardado localmente: {filename}[/yellow]")
    return "Local", filename


def generate_table(messages):
    """Genera la tabla de mensajes y archivos"""
    table = Table(title="IBM Options Data")
    table.add_column("Timestamp", justify="center", style="cyan")
    table.add_column("Number of Entries", justify="right", style="green")
    table.add_column("Last File Saved", justify="left", style="magenta")

    for msg in messages[-5:]:
        table.add_row(
            msg['timestamp'],
            f"{msg.get('entries_count', 0)}",
            msg.get('last_file', 'Not saved yet')
        )
    return table


def main():
    KAFKA_TOPIC = 'ibm_options'
    KAFKA_SERVER = 'localhost:9092'

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )

    messages = []

    # Ciclo infinito para procesar los mensajes de Kafka
    with Live(console=console, refresh_per_second=1) as live:
        for message in consumer:
            try:
                data = message.value
                if not isinstance(data, dict) or 'timestamp' not in data or 'data' not in data:
                    console.print("[red]Mensaje inválido recibido, omitiendo...[/red]")
                    continue

                timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

                # Guardar los datos en HDFS o localmente
                storage_type, filename = save_data(data, timestamp)
                data['last_file'] = f"{storage_type}: {filename}"

                # Añadir los datos procesados a la lista de mensajes
                messages.append(data)

                # Actualizar la tabla en tiempo real
                live.update(generate_table(messages))
            except Exception as e:
                console.print(f"[red]Error procesando el mensaje: {str(e)}[/red]")


if __name__ == "__main__":
    console.print("[yellow]Starting IBM Options Data Consumer...[/yellow]")
    try:
        main()
    except KeyboardInterrupt:
        console.print("[red]Consumer stopped by user[/red]")
