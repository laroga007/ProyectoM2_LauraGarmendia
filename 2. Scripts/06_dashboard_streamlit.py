"""
FleetLogix - Panel de entregas (extra credit)
Panel interactivo en Streamlit sobre el Data Warehouse del avance 3
(modelo estrella en Snowflake).

Ejecución, desde la carpeta del proyecto:
    pip install streamlit snowflake-connector-python cryptography
    streamlit run "2. Scripts/06_dashboard_streamlit.py"
El panel se abre en el navegador (http://localhost:8501). Para cerrarlo: Ctrl + C en la Terminal.

Credenciales: el mismo archivo de configuración que usa el ETL (05_etl_pipeline.py),
fuera del repositorio, con autenticación por par de claves (sin contraseña).

Reparto de tareas:
    * SQL, en Snowflake: une la tabla de hechos con sus dimensiones, filtra y agrega.
    * Python, con Streamlit: arma los filtros, envía las consultas con parámetros
      y muestra los resultados como indicadores, gráficos y tablas.
"""

import json
import os
from decimal import Decimal
from pathlib import Path

import altair as alt          # viene instalado con Streamlit
import pandas as pd
import streamlit as st

# =====================================================
# 1. CONFIGURACIÓN
# =====================================================

CONFIG_FILE = Path(
    os.environ.get(
        "SNOWFLAKE_CONFIG_FILE",
        Path.home() / "Desktop" / "snowflake_keys" / "snowflake_config.json",
    )
)
SNOWFLAKE_OBJECTS = {
    "warehouse": "FLEETLOGIX_WH",
    "database": "FLEETLOGIX_DW",
    "schema": "ANALYTICS",
    "role": "ACCOUNTADMIN",
}

st.set_page_config(page_title="FleetLogix · Panel de entregas", page_icon="🚚", layout="wide")


# =====================================================
# 2. CONEXIÓN Y CONSULTAS
# =====================================================

@st.cache_resource
def conectar():
    """Abre una sola conexión a Snowflake y la reutiliza mientras el panel esté abierto."""
    import snowflake.connector
    from cryptography.hazmat.primitives import serialization

    cfg = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
    with open(os.path.expanduser(cfg["private_key_file"]), "rb") as k:
        clave = serialization.load_pem_private_key(k.read(), password=None)
    return snowflake.connector.connect(
        account=cfg["account"],
        user=cfg["user"],
        authenticator="SNOWFLAKE_JWT",
        private_key=clave.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        ),
        **SNOWFLAKE_OBJECTS,
    )


@st.cache_data(ttl=600)
def consulta(sql, params=()):
    """Ejecuta una consulta en Snowflake y devuelve un DataFrame.

    El resultado queda guardado 10 minutos: si se repiten los mismos filtros,
    no se vuelve a consultar (y no se consume tiempo del warehouse).
    """
    cur = conectar().cursor()
    try:
        cur.execute(sql, params)                       # los %s se reemplazan por params
        columnas = [c[0].lower() for c in cur.description]   # Snowflake devuelve los nombres en mayúsculas
        df = pd.DataFrame(cur.fetchall(), columns=columnas)
    finally:
        cur.close()
    # Los DECIMAL llegan como Decimal de Python: se pasan a float para calcular y graficar
    for col in df.columns:
        if df[col].map(lambda v: isinstance(v, Decimal)).any():
            df[col] = df[col].astype(float)
    return df


def condiciones(desde, hasta, ciudades, tipos):
    """Arma el WHERE según los filtros elegidos. Los valores viajan como parámetros (%s),
    nunca pegados dentro del texto SQL."""
    partes = ["d.full_date BETWEEN %s AND %s"]
    params = [desde, hasta]
    if ciudades:
        partes.append(f"r.origin_city IN ({', '.join(['%s'] * len(ciudades))})")
        params += ciudades
    if tipos:
        partes.append(f"v.vehicle_type IN ({', '.join(['%s'] * len(tipos))})")
        params += tipos
    return " AND ".join(partes), tuple(params)


# Uniones del modelo estrella que comparten todas las consultas del panel
DESDE_ESTRELLA = """
    FROM fact_deliveries f
    JOIN dim_date    d ON d.date_key    = f.date_key
    JOIN dim_route   r ON r.route_key   = f.route_key
    JOIN dim_vehicle v ON v.vehicle_key = f.vehicle_key
"""
PUNTUALIDAD = "AVG(CASE WHEN f.is_on_time THEN 100.0 ELSE 0 END)"   # % de entregas a tiempo

SQL_INDICADORES = f"""
    SELECT COUNT(*)                    AS entregas,
           {PUNTUALIDAD}               AS puntualidad,
           AVG(f.delay_minutes)        AS retraso_prom,
           SUM(f.revenue_per_delivery) AS ingresos,
           SUM(f.cost_per_delivery)    AS costos
    {DESDE_ESTRELLA}
    WHERE {{where}}
"""
SQL_POR_MES = f"""
    SELECT d.year AS anio, d.month_num AS mes,
           COUNT(*) AS entregas, {PUNTUALIDAD} AS puntualidad
    {DESDE_ESTRELLA}
    WHERE {{where}}
    GROUP BY d.year, d.month_num
    ORDER BY d.year, d.month_num
"""
SQL_POR_HORA = f"""
    SELECT t.hour AS hora, COUNT(*) AS entregas, {PUNTUALIDAD} AS puntualidad
    {DESDE_ESTRELLA}
    JOIN dim_time t ON t.time_key = f.scheduled_time_key
    WHERE {{where}}
    GROUP BY t.hour
    ORDER BY t.hour
"""
SQL_POR_VEHICULO = f"""
    SELECT v.vehicle_type AS tipo,
           COUNT(*) AS entregas,
           AVG(f.cost_per_delivery) AS costo_prom,
           AVG(f.fuel_efficiency_km_per_liter) AS km_por_litro
    {DESDE_ESTRELLA}
    WHERE {{where}}
    GROUP BY v.vehicle_type
    ORDER BY entregas DESC
"""
SQL_POR_RUTA = f"""
    SELECT r.route_code AS ruta,
           r.origin_city || ' → ' || r.destination_city AS trayecto,
           r.distance_km AS distancia_km,
           COUNT(*) AS entregas,
           {PUNTUALIDAD} AS puntualidad,
           AVG(f.delay_minutes) AS retraso_prom,
           SUM(f.revenue_per_delivery) / 1000000 AS ingresos_m,
           (SUM(f.revenue_per_delivery) - SUM(f.cost_per_delivery))
               / SUM(f.revenue_per_delivery) * 100 AS margen
    {DESDE_ESTRELLA}
    WHERE {{where}}
    GROUP BY r.route_code, r.origin_city, r.destination_city, r.distance_km
    ORDER BY margen
"""


def num(valor, decimales=0):
    """Formato argentino/colombiano: punto para miles y coma para decimales."""
    texto = f"{valor:,.{decimales}f}"
    return texto.replace(",", "X").replace(".", ",").replace("X", ".")


def pesos(valor):
    """Montos grandes abreviados para que entren en el indicador: M = millones."""
    if valor >= 1e9:
        return f"$ {num(valor / 1e9, 1)} mil M"
    return f"$ {num(valor / 1e6, 1)} M"


# =====================================================
# 3. FILTROS (barra lateral)
# =====================================================

rango = consulta("""
    SELECT MIN(d.full_date) AS desde, MAX(d.full_date) AS hasta
    FROM fact_deliveries f JOIN dim_date d ON d.date_key = f.date_key
""")
primera, ultima = rango.loc[0, "desde"], rango.loc[0, "hasta"]
ciudades = consulta("SELECT DISTINCT origin_city FROM dim_route ORDER BY origin_city")["origin_city"].tolist()
tipos = consulta("SELECT DISTINCT vehicle_type FROM dim_vehicle ORDER BY vehicle_type")["vehicle_type"].tolist()

with st.sidebar:
    st.header("Filtros")
    fechas = st.date_input("Período", value=(primera, ultima),
                           min_value=primera, max_value=ultima, format="DD/MM/YYYY")
    sel_ciudades = st.multiselect("Ciudad de origen", ciudades, placeholder="Todas")
    sel_tipos = st.multiselect("Tipo de vehículo", tipos, placeholder="Todos")
    st.caption("Sin selección, se incluyen todas las opciones.")

if len(fechas) != 2:          # mientras se elige el rango, el calendario devuelve una sola fecha
    st.info("Seleccionar la fecha final del período.")
    st.stop()

where, params = condiciones(fechas[0], fechas[1], sel_ciudades, sel_tipos)


# =====================================================
# 4. INDICADORES PRINCIPALES
# =====================================================

st.title("🚚 FleetLogix · Panel de entregas")
st.caption(f"Data Warehouse en Snowflake (modelo estrella, avance 3) · "
           f"{fechas[0]:%d/%m/%Y} al {fechas[1]:%d/%m/%Y}")

ind = consulta(SQL_INDICADORES.format(where=where), params).iloc[0]
if not ind["entregas"]:
    st.warning("No hay entregas con los filtros elegidos.")
    st.stop()

margen = (ind["ingresos"] - ind["costos"]) / ind["ingresos"] * 100
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Entregas", num(ind["entregas"]))
c2.metric("Puntualidad", f"{num(ind['puntualidad'], 1)} %",
          help="Entregas con 30 minutos de retraso o menos (criterio de los avances 1 a 3).")
c3.metric("Retraso promedio", f"{num(ind['retraso_prom'], 1)} min")
c4.metric("Ingresos estimados", pesos(ind["ingresos"]),
          help="En pesos colombianos, con la tarifa supuesta en el ETL. "
               "M = millones; mil M = miles de millones.")
c5.metric("Margen estimado", f"{num(margen, 1)} %",
          help="(Ingresos − costos de combustible y peajes) / ingresos, con los supuestos del ETL.")

st.divider()


# =====================================================
# 5. GRÁFICOS
# =====================================================

por_mes = consulta(SQL_POR_MES.format(where=where), params)
por_mes["periodo"] = por_mes["anio"].astype(str) + "-" + por_mes["mes"].astype(int).map("{:02d}".format)

col_a, col_b = st.columns(2)
with col_a:
    st.subheader("Entregas por mes")
    st.bar_chart(por_mes, x="periodo", y="entregas", x_label="Mes", y_label="Entregas")
with col_b:
    st.subheader("Puntualidad por mes (%)")
    # Escala fija de 0 a 100 %: con la escala automática, una variación chica parece grande
    linea = alt.Chart(por_mes).mark_line(point=True).encode(
        x=alt.X("periodo:N", title="Mes"),
        y=alt.Y("puntualidad:Q", title="% a tiempo", scale=alt.Scale(domain=[0, 100])),
        tooltip=[alt.Tooltip("periodo:N", title="Mes"),
                 alt.Tooltip("puntualidad:Q", title="% a tiempo", format=".1f")],
    )
    st.altair_chart(linea)
st.caption("El primer y el último mes del período pueden estar incompletos.")

por_hora = consulta(SQL_POR_HORA.format(where=where), params)
por_vehiculo = consulta(SQL_POR_VEHICULO.format(where=where), params)

col_c, col_d = st.columns(2)
with col_c:
    st.subheader("Puntualidad según la hora programada (%)")
    barras = alt.Chart(por_hora).mark_bar().encode(
        x=alt.X("hora:O", title="Hora programada de entrega"),
        y=alt.Y("puntualidad:Q", title="% a tiempo", scale=alt.Scale(domain=[0, 100])),
        tooltip=[alt.Tooltip("hora:O", title="Hora"),
                 alt.Tooltip("entregas:Q", title="Entregas", format=","),
                 alt.Tooltip("puntualidad:Q", title="% a tiempo", format=".1f")],
    )
    st.altair_chart(barras)
with col_d:
    st.subheader("Por tipo de vehículo")
    # Se redondea antes de mostrar; "localized" usa el formato del navegador (punto para miles)
    st.dataframe(
        por_vehiculo.round({"costo_prom": 0, "km_por_litro": 1}), hide_index=True,
        column_config={
            "tipo": st.column_config.TextColumn("Tipo"),
            "entregas": st.column_config.NumberColumn("Entregas", format="localized"),
            "costo_prom": st.column_config.NumberColumn("Costo promedio por entrega ($)", format="localized"),
            "km_por_litro": st.column_config.NumberColumn("Km por litro", format="localized"),
        },
    )

st.subheader("Rutas, de menor a mayor margen")
st.caption("La puntualidad es pareja entre rutas; lo que más cambia es el margen. Con la tarifa de ejemplo, "
           "que cobra por peso y no por distancia, las rutas largas dan pérdida. "
           "Las columnas se pueden reordenar con un clic en el encabezado.")
por_ruta = consulta(SQL_POR_RUTA.format(where=where), params)
st.dataframe(
    por_ruta.round({"distancia_km": 0, "retraso_prom": 1, "ingresos_m": 1, "margen": 1}), hide_index=True,
    column_config={
        "ruta": st.column_config.TextColumn("Ruta"),
        "trayecto": st.column_config.TextColumn("Origen → destino"),
        "distancia_km": st.column_config.NumberColumn("Distancia (km)", format="localized"),
        "entregas": st.column_config.NumberColumn("Entregas", format="localized"),
        "puntualidad": st.column_config.ProgressColumn("Puntualidad", min_value=0, max_value=100,
                                                       format="%.1f %%"),
        "retraso_prom": st.column_config.NumberColumn("Retraso promedio (min)", format="localized"),
        "ingresos_m": st.column_config.NumberColumn("Ingresos estimados (millones $)", format="localized"),
        "margen": st.column_config.NumberColumn("Margen estimado (%)", format="localized",
                                                help="Negativo: la ruta da pérdida con la tarifa de ejemplo."),
    },
)

with st.expander("Ver las consultas SQL del panel"):
    st.caption("Todas parten de la tabla de hechos y la unen con sus dimensiones. "
               "Los %s son los parámetros que llegan desde los filtros.")
    st.code(SQL_INDICADORES.format(where=where), language="sql")
    st.code(SQL_POR_RUTA.format(where=where), language="sql")

st.caption("Fuente: FLEETLOGIX_DW.ANALYTICS en Snowflake, cargado por el ETL del avance 3. "
           "Ingresos y costos son estimaciones con los supuestos definidos en 05_etl_pipeline.py.")
