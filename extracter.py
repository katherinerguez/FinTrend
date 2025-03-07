import streamlit as st
import pandas as pd
import json
import matplotlib.pyplot as plt
import plotly.express as px
from hdfs import InsecureClient

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

st.sidebar.header("Filtros")
symbol_filter = st.sidebar.multiselect("Selecciona símbolos", options=data["symbol"].unique())
type_filter = st.sidebar.multiselect("Selecciona tipo de opción", options=data["type"].unique())
min_date = data["date"].min()
max_date = data["date"].max()
start_date = st.sidebar.date_input("Fecha inicial", min_date)
end_date = st.sidebar.date_input("Fecha final", max_date)

filtered_data = data.copy()
if symbol_filter:
    filtered_data = filtered_data[filtered_data["symbol"].isin(symbol_filter)]
if type_filter:
    filtered_data = filtered_data[filtered_data["type"].isin(type_filter)]
filtered_data = filtered_data[
    (filtered_data["date"] >= pd.to_datetime(start_date)) &
    (filtered_data["date"] <= pd.to_datetime(end_date))
]

if filtered_data.empty:
    st.warning("No hay datos disponibles para los filtros seleccionados.")
else:
    st.subheader("Datos filtrados")
    st.dataframe(filtered_data)

st.header("Métricas Financieras")

metric_column = st.selectbox("Selecciona la métrica financiera", numeric_columns)

fig = px.line(
    filtered_data,
    x="date",
    y=metric_column,
    color="symbol",
    title=f"Evolución de {metric_column} en el tiempo",
    labels={metric_column: metric_column, "date": "Fecha"}
)
st.plotly_chart(fig, use_container_width=True)

st.subheader("Estadísticas Descriptivas")
stats = filtered_data[numeric_columns].describe().T
st.table(stats)

st.subheader("Distribución de la métrica seleccionada")
fig_hist = px.histogram(
    filtered_data,
    x=metric_column,
    nbins=30,
    title=f"Distribución de {metric_column}"
)
st.plotly_chart(fig_hist, use_container_width=True)