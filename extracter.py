import streamlit as st
import pandas as pd
import json
import os
import plotly.express as px
import plotly.graph_objects as go
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import StandardScaler
from datetime import datetime, timedelta
import dask.dataframe as dd
import dask.bag as db
from hdfs import InsecureClient

st.write(1)
# Configuración de la página
#st.set_page_config(page_title="Análisis Histórico Financiero", layout="wide")
st.title("📊 Análisis Histórico de Datos Financieros")
st.write("En la era de la información, los datos financieros históricos son tu activo más valioso, donde analizar, comprender y aprovechar el pasado puede ayudarte a construir un futuro financiero sólido."
"Este estudio lo estaremos llevando hacia una de las empresas que alberga más patentes que ninguna otra empresa de tecnología de Estados Unidos, IBM (International Business Machines Corporation).")
#st.image("ibm.jpg", width=300)

def load_data(hdfs_url, directory_path):
    # Crear un cliente HDFS fuera del paralelismo
    client = InsecureClient(hdfs_url)

    # Listar archivos en el directorio de HDFS
    try:
        raw_list = client.list(directory_path, status=True)
    except Exception as e:
        st.error(f"Error al listar archivos en HDFS: {e}")
        return dd.DataFrame()  # Retorna un Dask DataFrame vacío en caso de error

    # Filtrar solo archivos no vacíos
    file_paths = [
        f"{directory_path}{name}"
        for name, metadata in raw_list
        if metadata['type'] == 'FILE' and metadata['length'] > 0
    ]

    # Si no hay archivos válidos, retorna un DataFrame vacío inmediatamente
    if not file_paths:
        st.warning("No se encontraron archivos JSON válidos en el directorio especificado.")
        return dd.DataFrame()

    # Crear una función independiente para leer un archivo
    def read_json_file(file_path):
      try:
        with client.read(file_path) as reader:
            # Verificar si el archivo está vacío
            content = reader.read()
            if not content.strip():
                st.warning(f"El archivo {file_path} está vacío.")
                return []

            # Intentar cargar el JSON
            raw_data = json.loads(content)
            # Verificar si contiene las claves 'data' y 'timestamp'
            if "data" in raw_data and "timestamp" in raw_data:
                # Agregar el 'timestamp' a cada registro en 'data'
                for record in raw_data["data"]:
                    record["timestamp"] = raw_data["timestamp"]
                return raw_data["data"]
            else:
                st.warning(f"El archivo {file_path} no contiene la clave 'data' o 'timestamp'.")
                return []
      except json.JSONDecodeError as e:
        return []
      except Exception as e:
        st.warning(f"Error al leer el archivo {file_path}: {e}")
        return []

    # Procesar los archivos usando una lista en lugar de un Bag
    all_data = []
    for file_path in file_paths:
        all_data.extend(read_json_file(file_path))

    # Verificar si hay datos válidos
    if not all_data:
        st.warning("No se encontraron datos válidos en los archivos JSON.")
        return dd.DataFrame()

    # Convertir los datos en un Dask DataFrame
    try:
        df = dd.from_pandas(pd.DataFrame(all_data), npartitions=1)
    except Exception as e:
        st.error(f"Error al convertir los datos en DataFrame: {e}")
        return dd.DataFrame()

    return df

# Parámetros para la carpeta local
directory_path = "/user/data/ibm_options/"  

HDFS_URL = 'http://172.19.0.4:9870'

try:
    data = load_data(HDFS_URL,directory_path)
except Exception as e:
    st.error(f"Error al cargar los datos desde la carpeta local: {e}")
    st.stop()

# Verificar si hay datos
if data.shape[0].compute() == 0:  # Verifica si hay filas en el DataFrame
    st.error("No se encontraron datos válidos en los archivos JSON.")
    st.stop()

# Convertir columnas numéricas a tipos adecuados
numeric_columns = ["strike", "last", "mark", "bid", "ask", "volume", "open_interest",
                   "implied_volatility", "delta", "gamma", "theta", "vega", "rho"]
for col in numeric_columns:
    if col in data.columns:
        data[col] = dd.to_numeric(data[col], errors="coerce")

# Convertir la columna 'timestamp' a datetime
if 'timestamp' in data.columns:
    data['timestamp'] = dd.to_datetime(data['timestamp'], errors="coerce")
else:
    st.warning("No se encontró la columna 'timestamp' en los datos. Se agregará una columna simulada.")
    data['timestamp'] = dd.from_pandas(pd.Series([datetime.now() - timedelta(minutes=i) for i in range(len(data))]), npartitions=1)

# Obtener el tiempo mínimo y máximo de los datos
min_timestamp = data['timestamp'].min().compute()
max_timestamp = data['timestamp'].max().compute()

# Filtrado de datos
st.sidebar.header("Filtros")
type_filter = st.sidebar.multiselect("Selecciona tipo de opción", options=data["type"].unique().compute())

# Aplicar filtros
filtered_data = data
if type_filter:
    filtered_data = filtered_data[filtered_data["type"].isin(type_filter)]

if filtered_data.shape[0].compute() == 0:
    st.warning("No hay datos disponibles para los filtros seleccionados.")

# Seleccionar rango de tiempo basado en los datos disponibles
st.sidebar.header("Rango de Tiempo")
st.sidebar.write(f"Rango de tiempo disponible: {min_timestamp} a {max_timestamp}")

# Opciones de rango de tiempo
time_options = {
    "Últimos 5 minutos": timedelta(minutes=5),
    "Últimos 15 minutos": timedelta(minutes=15),
    "Últimos 30 minutos": timedelta(minutes=30),
    "Todo": max_timestamp - min_timestamp
}

# Seleccionar rango de tiempo
selected_range = st.sidebar.selectbox("Selecciona el rango de tiempo", list(time_options.keys()))

# Calcular el tiempo de inicio basado en la selección del usuario
if selected_range == "Todo":
    start_time = min_timestamp
else:
    start_time = max_timestamp - time_options[selected_range]

if start_time < min_timestamp:
    st.sidebar.warning(f"El rango seleccionado no está completamente disponible. Ajustando al tiempo mínimo: {min_timestamp}")
    start_time = min_timestamp

# Filtrar datos por rango de tiempo
filtered_data = filtered_data[filtered_data['timestamp'] >= start_time]

# Visualización de métricas
st.subheader("Gráficos de Métricas Financieras")

# Gráfico de líneas para las métricas
st.write("A continuación se genera un gráfico de líneas que muestra la evolución temporal de una métrica financiera seleccionada por el usuario."
"Con lo cual se puede apreciar patrones en las métricas, se puede llevar a cabo un monitoreo y ver cómo cambian y ayudar a identificar cuál es el mejor momento para venta o compra.")
metric_column = st.selectbox("Selecciona la métrica financiera a analizar", numeric_columns)

# Gráfico con timestamp en el eje x
fig = px.line(
    filtered_data.compute(),
    x='timestamp',  # Usar la columna 'timestamp' en el eje x
    y=metric_column,
    color="symbol",  
    title=f"Evolución de {metric_column}",
    labels={"timestamp": "Tiempo", metric_column: metric_column}
)
st.plotly_chart(fig, use_container_width=True)

# Distribución de la métrica seleccionada
st.subheader("Distribución de la Métrica Seleccionada")
st.write("De acuerdo a la métrica seleccionada anteriormente se muestra un histograma que representa su distribución. Evalúa si los precios o volúmenes siguen una distribución normal o tienen sesgos. "
"Se detecta outliers en las métricas y analizar la dispersión de métricas como volatilidad implícita para medir incertidumbre.")
fig_hist = px.histogram(
    filtered_data.compute(),
    x=metric_column,
    nbins=30,
    title=f"Distribución de {metric_column}"
)
st.plotly_chart(fig_hist, use_container_width=True)

# Comparar contratos Call y Put
st.subheader("Comparación entre Opciones Call y Put")
st.write("Compara el volumen y el interés abierto (open interest) entre opciones Call y Put. Se puede apreciar los sentimientos del mercado que señala que altos volúmenes en Call pueden indicar expectativas alcistas"
"y altos volúmenes en Put pueden indicar expectativas bajistas. El open interest ayuda a evaluar la liquidez de las opciones.")
call_put_comparison = filtered_data.groupby("type")[["volume", "open_interest"]].sum().reset_index().compute()
fig_bar = px.bar(
    call_put_comparison,
    x="type",
    y=["volume", "open_interest"],
    barmode="group",
    title="Volumen y Open Interest"
)
st.plotly_chart(fig_bar, use_container_width=True)

data['expiration'] = dd.to_datetime(data['expiration'])

# Calcular días hasta la expiración
data['days_to_expiry'] = (data['expiration'] - datetime.now()).dt.days

# Preparación de datos para el modelo
features = ['strike', 'implied_volatility', 'days_to_expiry', 'delta', 'gamma', 'theta', 'vega', 'rho']
X = data[features]
y = data['last']  # Usamos 'last' como variable objetivo (precio de la opción)

# Eliminar filas con valores nulos
X = X.dropna()
y = y[X.index]

# Normalización de características
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X.compute())

# División de datos
X_train, X_test, y_train, y_test = train_test_split(X_scaled, y.compute(), test_size=0.2, random_state=42)

# Entrenamiento del modelo
model = RandomForestRegressor(random_state=42)
model.fit(X_train, y_train)

# Interfaz de usuario para predicciones
st.sidebar.header("Parámetros de Entrada")
strike = st.sidebar.number_input("Strike Price", value=100.0)
volatility = st.sidebar.number_input("Volatilidad Implícita", value=0.2)
days_to_expiry = st.sidebar.number_input("Días hasta la Expiración", value=30)
delta = st.sidebar.number_input("Delta", value=0.5)
gamma = st.sidebar.number_input("Gamma", value=0.01)
theta = st.sidebar.number_input("Theta", value=-0.05)
vega = st.sidebar.number_input("Vega", value=0.1)
rho = st.sidebar.number_input("Rho", value=0.01)

# Predicción
if st.sidebar.button("Predecir"):
    input_data = pd.DataFrame({
        'strike': [strike],
        'implied_volatility': [volatility],
        'days_to_expiry': [days_to_expiry],
        'delta': [delta],
        'gamma': [gamma],
        'theta': [theta],
        'vega': [vega],
        'rho': [rho]
    })
    input_scaled = scaler.transform(input_data)
    prediction = model.predict(input_scaled)
    st.write(f"El precio predicho de la opción es: {prediction[0]:.2f}")

st.subheader("Predicciones vs Valores Reales")
# Evaluación del modelo
y_pred = model.predict(X_test)
mse = mean_squared_error(y_test, y_pred)
st.write("Se utilizan los datos históricos y modelos avanzados para identificar tendencias y patrones ocultos, ofreciéndote una ventaja estratégica en tus decisiones de inversión. "
        f"Como modelo se emplea Random Forest. Es un modelo robusto que maneja bien relaciones no lineales entre las características y el objetivo. Esto es importante en finanzas, donde las relaciones entre variables pueden ser complejas. Al evaluar el modelo el Error Cuadrático Medio (MSE) fue de {mse:.2f}, por lo que el modelo es bueno")

# Gráfico de predicciones vs valores reales
fig = px.scatter(
    x=y_test,
    y=y_pred,
    labels={'x': 'Valores Reales', 'y': 'Predicciones'},
    title="Comparación entre Valores Reales y Predicciones"
)
fig.add_trace(go.Scatter(x=[min(y_test), max(y_test)], y=[min(y_test), max(y_test)], mode='lines', name='Línea Ideal'))
st.plotly_chart(fig, use_container_width=True)

# Gráfico de importancia de características
st.subheader("Importancia de las Características")
st.write("Para mostrar la importancia relativa de cada característica en las predicciones del modelo se utiliza un gráfico de barras. Esto nos ayuda a entender qué factores tienen más influencia en el precio de la opción.")
importances = model.feature_importances_
feature_importance_df = pd.DataFrame({'Feature': features, 'Importance': importances})
fig_importance = px.bar(
    feature_importance_df,
    x='Feature',
    y='Importance',
    title="Importancia de las Características en el Modelo"
)
st.plotly_chart(fig_importance, use_container_width=True)

