import streamlit as st
import pandas as pd
import json
import matplotlib.pyplot as plt
import plotly.express as px
from hdfs import InsecureClient
import os
from datetime import datetime
import plotly.graph_objects as go
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import StandardScaler

st.set_page_config(page_title="Análisis Histórico Financiero", layout="wide")
st.title("📊 Análisis de Opciones Financieras")

# Conexión a HDFS
@st.cache_data
def load_data_from_hdfs(hdfs_url, directory_path):
    client = InsecureClient(hdfs_url)
    all_data = []

    try:
        raw_list = client.list(directory_path, status=True)
        for name, metadata in raw_list:
            if metadata['type'] == 'FILE' and metadata['length'] > 0:
                st.info(f"Leyendo archivo: {name}")
                with client.read(f"{directory_path}{name}") as reader:
                    try:
                        raw_data = json.load(reader)
                        if "data" in raw_data:
                            all_data.extend(raw_data["data"])
                        else:
                            st.warning(f"El archivo {name} no contiene la clave 'data'.")
                    except json.JSONDecodeError as e:
                        st.warning(f"Error al decodificar JSON en el archivo {name}: {e}")
            elif metadata['type'] == 'FILE' and metadata['length'] == 0:
                st.warning(f"Archivo vacío omitido: {name}")
    except Exception as e:
        st.error(f"Error al listar archivos en HDFS: {e}")
        return pd.DataFrame()

    df = pd.DataFrame(all_data)
    st.info(f"Total de registros cargados: {len(df)}")
    return df

hdfs_url = "http://localhost:9870"  
directory_path = "/user/data/ibm_options/" 

data = pd.DataFrame()

try:
    data = load_data_from_hdfs(hdfs_url, directory_path)
    if not data.empty:
        st.success("Datos cargados exitosamente desde HDFS.")
    else:
        st.warning("No se encontraron datos válidos en HDFS.")
except Exception as e:
    st.error(f"Error al cargar los datos desde HDFS: {e}")

if data.empty:
    st.error("No se encontraron datos válidos en los archivos JSON.")
    st.stop()

numeric_columns = ["strike", "last", "mark", "bid", "ask", "volume", "open_interest",
                   "implied_volatility", "delta", "gamma", "theta", "vega", "rho"]
for col in numeric_columns:
    if col in data.columns:
        data[col] = pd.to_numeric(data[col], errors="coerce")

date_columns = ["date", "expiration"]
for col in date_columns:
    if col in data.columns:
        data[col] = pd.to_datetime(data[col], errors="coerce")

# Filtrado de datos
st.sidebar.header("Filtros")
type_filter = st.sidebar.multiselect("Selecciona tipo de opción", options=data["type"].unique())

# Aplicar filtros
filtered_data = data.copy()
if type_filter:
    filtered_data = filtered_data[filtered_data["type"].isin(type_filter)]

if filtered_data.empty:
    st.warning("No hay datos disponibles para los filtros seleccionados.")
date_columns = ["expiration", "date"]
for col in date_columns:
    filtered_data[col] = pd.to_datetime(filtered_data[col], errors="coerce")

# Visualización de métricas
st.subheader("Gráficos de Métricas Financieras")

# Gráfico de líneas para las metricas
st.write("A continuación se genera un gráfico de líneas que muestra la evolución temporal de una métrica financiera seleccionada por el usuario."
"Con lo cual se puede apreciar patrones en las métricas, se puede llevar a cabo un monitoreo y ver cómo cambian y ayudar a identificar cuál es el mejor momento para venta o compra.")
metric_column = st.selectbox("Selecciona la métrica financiera a analizar", numeric_columns)
fig = px.line(
    filtered_data,
    x=filtered_data.index,  
    y=metric_column,
    color="symbol",  
    title=f"Evolución de {metric_column}",
    labels={"index": "Índice", metric_column: metric_column}
)
st.plotly_chart(fig, use_container_width=True)

# Distribución de la métrica seleccionada
st.subheader("Distribución de la Métrica Seleccionada")
st.write("De acuerdo a la métrica seleccionada anteriormente se muestra un histograma que representa su distribución. Evalúa si los precios o volúmenes siguen una distribución normal o tienen sesgos. "
"Se detecta outliers en las métricas y analizar la dispersión de métricas como volatilidad implícita para medir incertidumbre.")
fig_hist = px.histogram(
    filtered_data,
    x=metric_column,
    nbins=30,
    title=f"Distribución de {metric_column}"
)
st.plotly_chart(fig_hist, use_container_width=True)

# Comparar contratos Call y Put
st.subheader("Comparación entre Opciones Call y Put")
st.write("Compara el volumen y el interés abierto (open interest) entre opciones Call y Put. Se puede apreciar los sentimientos del mercado que señala que altos volúmenes en Call pueden indicar expectativas alcistas"
"y altos volúmenes en Put pueden indicar expectativas bajistas. El open interest ayuda a evaluar la liquidez de las opciones.")
call_put_comparison = filtered_data.groupby("type")[["volume", "open_interest"]].sum().reset_index()
fig_bar = px.bar(
    call_put_comparison,
    x="type",
    y=["volume", "open_interest"],
    barmode="group",
    title="Volumen y Open Interest"
)
st.plotly_chart(fig_bar, use_container_width=True)

data['expiration'] = pd.to_datetime(data['expiration'])

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
X_scaled = scaler.fit_transform(X)

# División de datos
X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.2, random_state=42)

# Entrenamiento del modelo
model = RandomForestRegressor(random_state=42)
model.fit(X_train, y_train)

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
    st.write(f"El precio predicho de la opción es: **{prediction[0]:.2f}**")

st.subheader("Predicciones vs Valores Reales")
# Evaluación del modelo
y_pred = model.predict(X_test)
mse = mean_squared_error(y_test, y_pred)
st.write("Se utilizan los datos históricos y modelos avanzados para identificar tendencias y patrones ocultos, ofreciéndote una ventaja estratégica en tus decisiones de inversión. "
        f"Como modelo se emplea Random Forest. Es un modelo robusto que maneja bien relaciones no lineales entre las características y el objetivo. Esto es importante en finanzas, donde las relaciones entre variables pueden ser complejas. Al evaluar el modelo el Error Cuadrático Medio (MSE) fue de **{mse:.2f}**, por lo que el modelo es bueno")

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




