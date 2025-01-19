from datetime import datetime
import json
import os
import time

# Configuración de Rich
from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.layout import Layout

# Configuración de HDFS
from hdfs import InsecureClient

# Configuración de Kafka
from kafka import KafkaConsumer

console = Console()
layout = Layout()

# Configuración de HDFS
HDFS_URL = 'http://localhost:9870'
HDFS_DIR = '/user/data/ibm_options/'

def setup_hdfs():
    """Configura la conexión HDFS y crea el directorio si no existe"""
    try:
        client = InsecureClient(HDFS_URL, user='root')
        
        # Verificar si el directorio existe
        try:
            client.status(HDFS_DIR)
            console.print(f"[green]Directorio HDFS {HDFS_DIR} existe[/green]")
        except:
            # Crear el directorio y sus padres
            client.makedirs(HDFS_DIR)
            console.print(f"[yellow]Directorio HDFS {HDFS_DIR} creado[/yellow]")
        
        # Verificar permisos escribiendo un archivo de prueba
        test_file = f"{HDFS_DIR}test.txt"
        try:
            with client.write(test_file, encoding='utf-8') as writer:
                writer.write("test")
            client.delete(test_file)
            console.print("[green]Permisos de escritura verificados[/green]")
        except Exception as e:
            console.print(f"[red]Error de permisos: {str(e)}[/red]")
            return None
        
        return client
    except Exception as e:
        console.print(f"[red]Error de conexión con HDFS: {str(e)}[/red]")
        return None

# Configurar HDFS
hdfs_client = setup_hdfs()

# Directorio local como respaldo
LOCAL_DIR = './ibm_options_data/'
os.makedirs(LOCAL_DIR, exist_ok=True)

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
    
    # Fallback a almacenamiento local
    local_path = os.path.join(LOCAL_DIR, filename)
    with open(local_path, 'w', encoding='utf-8') as f:
        json.dump(data, f)
    console.print(f"[yellow]Archivo guardado localmente: {filename}[/yellow]")
    return "Local", filename

def generate_table(messages, files):
    """Genera la tabla de mensajes y archivos"""
    table = Table(title="IBM Options Data")
    table.add_column("Timestamp", justify="center", style="cyan")
    table.add_column("Number of Entries", justify="right", style="green")
    table.add_column("Last File Saved", justify="left", style="magenta")
    
    for msg in messages[-5:]:
        table.add_row(
            msg['timestamp'],
            f"{msg['entries_count']}",
            msg.get('last_file', 'Not saved yet')
        )
    return table

def main():
    KAFKA_TOPIC = 'ibm_options'
    KAFKA_SERVER = 'localhost:9092'
    
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    messages = []
    saved_files = []

    with Live(layout, refresh_per_second=1) as live:
        for message in consumer:
            data = message.value
            current_timestamp = int(time.time())
            
            storage_type, filename = save_data(data, current_timestamp)
            data['last_file'] = f"{storage_type}: {filename}"
            saved_files.append(filename)

            messages.append(data)
            layout.update(generate_table(messages, saved_files))
            live.refresh()

if __name__ == "__main__":
    console.print("[yellow]Starting IBM Options Data Consumer...[/yellow]")
    try:
        main()
    except KeyboardInterrupt:
        console.print("[red]Consumer stopped by user[/red]")
