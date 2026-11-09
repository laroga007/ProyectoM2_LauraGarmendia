"""
FleetLogix - Avance 3: Pipeline ETL
Extrae de PostgreSQL, transforma con pandas y carga en Snowflake (modelo estrella).

Modos de ejecución:
    python 05_etl_pipeline.py --full --until 2026-10-14   # carga histórica inicial (hasta el 13/10 inclusive)
    python 05_etl_pipeline.py --date 2026-10-14           # carga de un día puntual
    python 05_etl_pipeline.py                             # carga del día anterior (uso diario)
    python 05_etl_pipeline.py --schedule                  # queda corriendo y ejecuta todos los días a las 02:00

Credenciales (no se guardan en el código):
    * PostgreSQL: variable de entorno FLEETLOGIX_DB_PASSWORD; si no existe, se pide por teclado.
    * Snowflake: archivo JSON fuera del repositorio (por defecto
      ~/Desktop/snowflake_keys/snowflake_config.json) con account, user y
      private_key_file. La conexión usa autenticación por par de claves (sin contraseña).

Ajustes realizados sobre el script base:
    * Claves de dimensión reales en la tabla de hechos (antes se guardaban los ID
      de PostgreSQL y el cliente era siempre 1). La búsqueda de claves se hace en
      Snowflake y elige la versión de la dimensión vigente en la fecha de la entrega.
    * SCD tipo 2 implementado para conductores y vehículos (el script base
      consultaba un driver_id inexistente en staging_daily_load).
    * Carga histórica inicial además de la diaria: los datos terminan el 15/10/2026,
      así que "el día anterior" a hoy no tiene entregas.
    * Carga en bloque con write_pandas + MERGE (el script base insertaba fila por
      fila). El MERGE por delivery_id hace que re-ejecutar un día no duplique datos.
    * Entregas por viaje contadas sobre el viaje completo en PostgreSQL (el script
      base las contaba solo dentro del lote, y fallaba con viajes que cruzan la medianoche).
    * Distancia y combustible del viaje repartidos entre sus entregas, para que las
      sumas no se multipliquen.
    * delivery_time_minutes = tiempo desde la salida del viaje hasta la entrega;
      delay_minutes = minutos de retraso sobre la hora programada (el script base
      usaba la misma diferencia para las dos métricas).
    * Totales diarios (los dos #TO DO# del script base) con MERGE por fecha.
"""

import argparse
import getpass
import json
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2

# =====================================================
# CONFIGURACIÓN
# =====================================================

LOG_FILE = Path(__file__).resolve().parent / "etl_pipeline.log"
log = logging.getLogger("fleetlogix_etl")
if not log.handlers:   # logger propio: funciona igual desde la terminal y desde Jupyter
    log.setLevel(logging.INFO)
    log.propagate = False
    _fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    for _h in (logging.FileHandler(LOG_FILE, encoding="utf-8"), logging.StreamHandler(sys.stdout)):
        _h.setFormatter(_fmt)
        log.addHandler(_h)

POSTGRES_CONFIG = {
    "host": "localhost",
    "database": "fleetlogix",
    "user": "postgres",
    "port": 5432,
}

SNOWFLAKE_CONFIG_FILE = Path(
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

# Supuestos de negocio (no hay precios ni tarifas en el modelo de origen)
FUEL_PRICE_COP_PER_LITER = 5000      # costo del combustible por litro
REVENUE_BASE_COP = 20000             # tarifa base por entrega
REVENUE_PER_KG_COP = 500             # tarifa por kg
ON_TIME_TOLERANCE_MIN = 30           # mismo criterio que en los avances 1 y 2

# Umbrales de clasificación (definidos a partir de la distribución de los datos)
DRIVER_ON_TIME_HIGH = 0.875          # >= 87,5 % puntual -> 'Alto'
DRIVER_ON_TIME_LOW = 0.86            # <  86 % puntual   -> 'Bajo'
CUSTOMER_PREMIUM_MIN = 120           # >= 120 entregas   -> 'Premium'
CUSTOMER_OCASIONAL_MAX = 30          # <  30 entregas    -> 'Ocasional'
COMPANY_MARKERS = r"S\.A\.S\.|S\.A\.|Ltda|& Cía"

FIRST_VERSION_FROM = "1900-01-01"    # inicio de la primera versión de cada registro SCD2
OPEN_END = "9999-12-31"              # fin de la versión vigente


# =====================================================
# PIPELINE
# =====================================================

class FleetLogixETL:
    def __init__(self, start: date, end: date, label: str):
        """Procesa las entregas completadas con fecha de entrega en [start, end)."""
        self.start = start
        self.end = end
        self.effective_date = end - timedelta(days=1)   # fecha "de negocio" del lote
        self.label = label
        self.pg_conn = None
        self.sf_conn = None
        self.batch_id = int(datetime.now().timestamp())
        self.metrics = {
            "records_extracted": 0,
            "records_rejected": 0,
            "records_transformed": 0,
            "records_loaded": 0,
            "facts_inserted": 0,
            "facts_updated": 0,
            "facts_without_keys": 0,
            "scd2_new_versions": {},
            "errors": 0,
        }

    # -------------------------------------------------
    # Conexiones
    # -------------------------------------------------
    def connect_databases(self) -> bool:
        try:
            password = os.environ.get("FLEETLOGIX_DB_PASSWORD") or getpass.getpass(
                "Contraseña de PostgreSQL: "
            )
            self.pg_conn = psycopg2.connect(**POSTGRES_CONFIG, password=password)
            log.info("Conectado a PostgreSQL")

            import snowflake.connector  # se importa acá para que el resto del módulo no dependa de él

            with open(SNOWFLAKE_CONFIG_FILE, encoding="utf-8") as f:
                cfg = json.load(f)
            from cryptography.hazmat.primitives import serialization

            with open(os.path.expanduser(cfg["private_key_file"]), "rb") as k:
                key = serialization.load_pem_private_key(k.read(), password=None)
            self.sf_conn = snowflake.connector.connect(
                account=cfg["account"],
                user=cfg["user"],
                authenticator="SNOWFLAKE_JWT",
                private_key=key.private_bytes(
                    encoding=serialization.Encoding.DER,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption(),
                ),
                **SNOWFLAKE_OBJECTS,
            )
            log.info("Conectado a Snowflake")
            return True
        except Exception as e:
            log.error(f"Error en conexión: {e}")
            self.metrics["errors"] += 1
            return False

    def close_connections(self):
        for conn in (self.pg_conn, self.sf_conn):
            try:
                if conn:
                    conn.close()
            except Exception:
                pass
        log.info("Conexiones cerradas")

    # -------------------------------------------------
    # Utilidades
    # -------------------------------------------------
    def _pg_query(self, sql: str) -> pd.DataFrame:
        cur = self.pg_conn.cursor()
        cur.execute(sql)
        cols = [c[0] for c in cur.description]
        df = pd.DataFrame(cur.fetchall(), columns=cols)
        cur.close()
        return df

    def _sf_exec(self, sql: str):
        cur = self.sf_conn.cursor()
        try:
            cur.execute(sql)
            return cur.fetchall()
        finally:
            cur.close()

    def _upload(self, df: pd.DataFrame, table: str):
        """Sube un DataFrame a una tabla temporal de Snowflake (reemplaza si existe)."""
        from snowflake.connector.pandas_tools import write_pandas

        out = df.copy()
        for c in out.columns:
            # fechas y horas como texto ISO: Snowflake las convierte sin ambigüedad
            if pd.api.types.is_datetime64_any_dtype(out[c]):
                out[c] = out[c].dt.strftime("%Y-%m-%d %H:%M:%S")
            elif out[c].dtype == object:
                out[c] = out[c].map(lambda x: x.isoformat() if isinstance(x, date) else x)
        out.columns = [c.upper() for c in out.columns]
        ok, _, nrows, _ = write_pandas(
            self.sf_conn, out, table.upper(),
            auto_create_table=True, overwrite=True, table_type="temporary",
            quote_identifiers=False,
        )
        if not ok:
            raise RuntimeError(f"write_pandas falló para {table}")
        return nrows

    # -------------------------------------------------
    # EXTRACCIÓN
    # -------------------------------------------------
    def extract_daily_data(self) -> pd.DataFrame:
        """Entregas completadas en el período, con los datos del viaje y la ruta."""
        log.info(f"Extrayendo entregas del {self.start} al {self.effective_date}...")
        sql = f"""
            SELECT d.delivery_id, d.trip_id, d.tracking_number, d.customer_name,
                   d.delivery_address, d.package_weight_kg::float AS package_weight_kg,
                   d.scheduled_datetime, d.delivered_datetime, d.delivery_status,
                   d.recipient_signature,
                   t.vehicle_id, t.driver_id, t.route_id,
                   t.departure_datetime, t.arrival_datetime,
                   t.fuel_consumed_liters::float AS fuel_consumed_liters,
                   r.distance_km::float AS route_distance_km,
                   r.toll_cost::float   AS toll_cost,
                   r.destination_city,
                   v.capacity_kg::float AS capacity_kg,
                   n.deliveries_in_trip
            FROM deliveries d
            JOIN trips t    ON t.trip_id = d.trip_id
            JOIN routes r   ON r.route_id = t.route_id
            JOIN vehicles v ON v.vehicle_id = t.vehicle_id
            JOIN (SELECT trip_id, COUNT(*) AS deliveries_in_trip
                  FROM deliveries GROUP BY trip_id) n ON n.trip_id = d.trip_id
            WHERE d.delivery_status = 'delivered'
              AND d.delivered_datetime >= '{self.start.isoformat()}'
              AND d.delivered_datetime <  '{self.end.isoformat()}'
        """
        try:
            df = self._pg_query(sql)
            self.metrics["records_extracted"] = len(df)
            log.info(f"Extraídos {len(df):,} registros")
            return df
        except Exception as e:
            log.error(f"Error en extracción: {e}")
            self.metrics["errors"] += 1
            return pd.DataFrame()

    def extract_dimensions(self) -> dict:
        """Estado de las tablas maestras a la fecha del lote."""
        end = self.end.isoformat()
        dims = {}
        dims["vehicle"] = self._pg_query(f"""
            SELECT v.vehicle_id, v.license_plate, v.vehicle_type,
                   v.capacity_kg::float AS capacity_kg, v.fuel_type,
                   v.acquisition_date, v.status,
                   (SELECT MAX(m.maintenance_date) FROM maintenance m
                     WHERE m.vehicle_id = v.vehicle_id
                       AND m.maintenance_date < '{end}') AS last_maintenance_date
            FROM vehicles v
        """)
        dims["driver"] = self._pg_query(f"""
            SELECT dr.driver_id, dr.employee_code,
                   dr.first_name || ' ' || dr.last_name AS full_name,
                   dr.license_number, dr.license_expiry, dr.phone, dr.hire_date,
                   dr.status, p.on_time_rate
            FROM drivers dr
            LEFT JOIN (
                SELECT t.driver_id,
                       AVG((d.delivered_datetime <= d.scheduled_datetime
                            + INTERVAL '{ON_TIME_TOLERANCE_MIN} minutes')::int) AS on_time_rate
                FROM deliveries d JOIN trips t ON t.trip_id = d.trip_id
                WHERE d.delivery_status = 'delivered' AND d.delivered_datetime < '{end}'
                GROUP BY t.driver_id
            ) p ON p.driver_id = dr.driver_id
        """)
        dims["route"] = self._pg_query("""
            SELECT route_id, route_code, origin_city, destination_city,
                   distance_km::float AS distance_km,
                   estimated_duration_hours::float AS estimated_duration_hours,
                   toll_cost::float AS toll_cost
            FROM routes
        """)
        dims["customer"] = self._pg_query(f"""
            SELECT d.customer_name,
                   MODE() WITHIN GROUP (ORDER BY TRIM(SPLIT_PART(d.delivery_address, ',', 2))) AS city,
                   MIN(d.delivered_datetime)::date AS first_delivery_date,
                   COUNT(*) AS total_deliveries
            FROM deliveries d
            WHERE d.delivery_status = 'delivered' AND d.delivered_datetime < '{end}'
            GROUP BY d.customer_name
        """)
        return dims

    # -------------------------------------------------
    # TRANSFORMACIÓN
    # -------------------------------------------------
    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Métricas por entrega y controles de calidad."""
        log.info("Transformando entregas...")
        try:
            for c in ["scheduled_datetime", "delivered_datetime",
                      "departure_datetime", "arrival_datetime"]:
                df[c] = pd.to_datetime(df[c])
            n = len(df)

            # Tiempos
            df["delivery_time_minutes"] = (
                (df["delivered_datetime"] - df["departure_datetime"]).dt.total_seconds() / 60
            ).round()
            df["delay_minutes"] = (
                (df["delivered_datetime"] - df["scheduled_datetime"]).dt.total_seconds() / 60
            ).clip(lower=0).round()
            df["is_on_time"] = df["delay_minutes"] <= ON_TIME_TOLERANCE_MIN

            # Entregas por hora del viaje (viajes completados)
            trip_hours = (
                (df["arrival_datetime"] - df["departure_datetime"]).dt.total_seconds() / 3600
            )
            df["deliveries_per_hour"] = (df["deliveries_in_trip"] / trip_hours).round(2)

            # Eficiencia del viaje y costo estimado (supuestos definidos arriba)
            trip_fuel = df["fuel_consumed_liters"]
            df["fuel_efficiency_km_per_liter"] = (df["route_distance_km"] / trip_fuel).round(2)
            df["cost_per_delivery"] = (
                (trip_fuel * FUEL_PRICE_COP_PER_LITER + df["toll_cost"]) / df["deliveries_in_trip"]
            ).round(2)

            # Distancia y combustible del viaje repartidos entre sus entregas
            df["distance_km"] = (df["route_distance_km"] / df["deliveries_in_trip"]).round(2)
            df["fuel_consumed_liters"] = (trip_fuel / df["deliveries_in_trip"]).round(2)
            df["revenue_per_delivery"] = (
                REVENUE_BASE_COP + df["package_weight_kg"] * REVENUE_PER_KG_COP
            ).round(2)

            df["is_damaged"] = False                   # sin dato en el origen
            df["has_signature"] = df["recipient_signature"].fillna(False).astype(bool)

            # Controles de calidad
            checks = {
                "tiempo de entrega negativo": df["delivery_time_minutes"] < 0,
                "peso fuera de rango": (df["package_weight_kg"] <= 0)
                                       | (df["package_weight_kg"] > df["capacity_kg"]),
                "combustible no válido": df["fuel_consumed_liters"] <= 0,
            }
            # Entregas ya completadas de viajes todavía en curso: son válidas, pero
            # las métricas del viaje (duración, combustible) quedan vacías
            in_progress = df["arrival_datetime"].isna().sum()
            if in_progress:
                log.info(f"{in_progress} entregas de viajes en curso: sin métricas de viaje")
            bad = pd.Series(False, index=df.index)
            for name, mask in checks.items():
                if mask.sum():
                    log.warning(f"Calidad: {mask.sum():,} registros con {name}")
                bad |= mask
            df = df[~bad].copy()
            self.metrics["records_rejected"] = int(bad.sum())

            for c in ["delivery_time_minutes", "delay_minutes"]:
                df[c] = df[c].astype(int)

            self.metrics["records_transformed"] = len(df)
            log.info(f"Transformados {len(df):,} de {n:,} registros "
                     f"({self.metrics['records_rejected']} rechazados)")
            return df
        except Exception as e:
            log.error(f"Error en transformación: {e}")
            self.metrics["errors"] += 1
            return pd.DataFrame()

    def transform_dimensions(self, dims: dict) -> dict:
        """Atributos derivados de las dimensiones."""
        ref = pd.Timestamp(self.effective_date)

        def months_since(s):
            s = pd.to_datetime(s)
            return ((ref.year - s.dt.year) * 12 + (ref.month - s.dt.month)).astype("Int64")

        v = dims["vehicle"]
        v["age_months"] = months_since(v["acquisition_date"])

        d = dims["driver"]
        d["experience_months"] = months_since(d["hire_date"])
        rate = pd.to_numeric(d["on_time_rate"], errors="coerce")
        d["performance_category"] = np.select(
            [rate.isna(), rate >= DRIVER_ON_TIME_HIGH, rate < DRIVER_ON_TIME_LOW],
            ["Sin viajes", "Alto", "Bajo"], default="Medio",
        )
        d.drop(columns="on_time_rate", inplace=True)

        r = dims["route"]
        r["difficulty_level"] = pd.cut(
            r["distance_km"], bins=[0, 300, 800, np.inf], labels=["Fácil", "Medio", "Difícil"]
        ).astype(str)
        # todas las rutas unen ciudades distintas
        r["route_type"] = np.where(r["origin_city"] == r["destination_city"], "Urbana", "Interurbana")

        c = dims["customer"]
        c["customer_type"] = np.where(
            c["customer_name"].str.contains(COMPANY_MARKERS, regex=True), "Empresa", "Individual"
        )
        tot = pd.to_numeric(c["total_deliveries"])
        c["customer_category"] = np.select(
            [tot >= CUSTOMER_PREMIUM_MIN, tot < CUSTOMER_OCASIONAL_MAX],
            ["Premium", "Ocasional"], default="Regular",
        )
        return dims

    # -------------------------------------------------
    # CARGA
    # -------------------------------------------------
    def load_staging(self, raw: pd.DataFrame):
        """Guarda las entregas extraídas, tal como llegaron, en formato JSON (VARIANT)."""
        self._upload(raw, "tmp_raw")
        self._sf_exec(f"""
            INSERT INTO staging_daily_load (raw_data, etl_batch_id)
            SELECT OBJECT_CONSTRUCT(*), {self.batch_id} FROM tmp_raw
        """)
        log.info(f"Staging: {len(raw):,} registros guardados como JSON")

    def _scd2(self, table, src, natural_key, tracked, type1):
        """SCD tipo 2: cierra la versión vigente si cambió un atributo seguido y
        agrega la nueva versión. Los atributos tipo 1 se actualizan en la vigente."""
        eff = self.effective_date.isoformat()
        changed = " OR ".join(f"d.{c} IS DISTINCT FROM s.{c}" for c in tracked)

        # 1) Cerrar versiones que cambiaron
        res = self._sf_exec(f"""
            UPDATE {table} d
               SET valid_to = DATEADD(day, -1, '{eff}'::DATE), is_current = FALSE
              FROM {src} s
             WHERE d.{natural_key} = s.{natural_key} AND d.is_current
               AND d.valid_from < '{eff}'::DATE
               AND ({changed})
        """)
        closed = res[0][0] if res else 0

        # 2) Actualizar atributos tipo 1 en la versión vigente
        if type1:
            sets = ", ".join(f"{c} = s.{c}" for c in type1)
            self._sf_exec(f"""
                UPDATE {table} d SET {sets}
                  FROM {src} s
                 WHERE d.{natural_key} = s.{natural_key} AND d.is_current
            """)

        # 3) Insertar versiones nuevas (registros nuevos o que cambiaron)
        cols = [natural_key] + tracked + type1
        col_list = ", ".join(cols)
        sel = ", ".join(f"s.{c}" for c in cols)
        res = self._sf_exec(f"""
            INSERT INTO {table} ({col_list}, valid_from, valid_to, is_current)
            SELECT {sel},
                   IFF(h.{natural_key} IS NULL, '{FIRST_VERSION_FROM}'::DATE, '{eff}'::DATE),
                   '{OPEN_END}'::DATE, TRUE
              FROM {src} s
              LEFT JOIN (SELECT DISTINCT {natural_key} FROM {table}) h
                     ON h.{natural_key} = s.{natural_key}
             WHERE NOT EXISTS (SELECT 1 FROM {table} d
                                WHERE d.{natural_key} = s.{natural_key} AND d.is_current)
        """)
        inserted = res[0][0] if res else 0
        self.metrics["scd2_new_versions"][table] = int(closed)
        log.info(f"{table}: {inserted} versiones insertadas ({closed} por cambios)")

    def load_dimensions(self, dims: dict):
        log.info("Cargando dimensiones...")
        try:
            # Vehículo y conductor: SCD tipo 2
            self._upload(dims["vehicle"], "tmp_dim_vehicle")
            self._sf_exec("""CREATE OR REPLACE TEMPORARY TABLE tmp_v AS
                SELECT vehicle_id::INT AS vehicle_id, license_plate, vehicle_type,
                       capacity_kg::DECIMAL(10,2) AS capacity_kg, fuel_type,
                       acquisition_date::DATE AS acquisition_date, status,
                       age_months::INT AS age_months,
                       TRY_TO_DATE(last_maintenance_date) AS last_maintenance_date
                FROM tmp_dim_vehicle""")
            self._scd2("dim_vehicle", "tmp_v", "vehicle_id",
                       tracked=["license_plate", "vehicle_type", "capacity_kg",
                                "fuel_type", "acquisition_date", "status"],
                       type1=["age_months", "last_maintenance_date"])

            self._upload(dims["driver"], "tmp_dim_driver")
            self._sf_exec("""CREATE OR REPLACE TEMPORARY TABLE tmp_d AS
                SELECT driver_id::INT AS driver_id, employee_code, full_name, license_number,
                       license_expiry::DATE AS license_expiry, phone,
                       hire_date::DATE AS hire_date, status, performance_category,
                       experience_months::INT AS experience_months
                FROM tmp_dim_driver""")
            self._scd2("dim_driver", "tmp_d", "driver_id",
                       tracked=["employee_code", "full_name", "license_number",
                                "license_expiry", "phone", "hire_date", "status",
                                "performance_category"],
                       type1=["experience_months"])

            # Ruta y cliente: SCD tipo 1 (MERGE: actualiza o inserta)
            self._upload(dims["route"], "tmp_dim_route")
            self._sf_exec("""
                MERGE INTO dim_route d USING tmp_dim_route s ON d.route_id = s.route_id
                WHEN MATCHED THEN UPDATE SET
                    route_code = s.route_code, origin_city = s.origin_city,
                    destination_city = s.destination_city, distance_km = s.distance_km,
                    estimated_duration_hours = s.estimated_duration_hours,
                    toll_cost = s.toll_cost, difficulty_level = s.difficulty_level,
                    route_type = s.route_type
                WHEN NOT MATCHED THEN INSERT
                    (route_id, route_code, origin_city, destination_city, distance_km,
                     estimated_duration_hours, toll_cost, difficulty_level, route_type)
                VALUES (s.route_id, s.route_code, s.origin_city, s.destination_city,
                        s.distance_km, s.estimated_duration_hours, s.toll_cost,
                        s.difficulty_level, s.route_type)
            """)

            self._upload(dims["customer"], "tmp_dim_customer")
            self._sf_exec("""
                MERGE INTO dim_customer d USING tmp_dim_customer s
                   ON d.customer_name = s.customer_name
                WHEN MATCHED THEN UPDATE SET
                    customer_type = s.customer_type, city = s.city,
                    first_delivery_date = s.first_delivery_date::DATE,
                    total_deliveries = s.total_deliveries,
                    customer_category = s.customer_category
                WHEN NOT MATCHED THEN INSERT
                    (customer_name, customer_type, city, first_delivery_date,
                     total_deliveries, customer_category)
                VALUES (s.customer_name, s.customer_type, s.city,
                        s.first_delivery_date::DATE, s.total_deliveries, s.customer_category)
            """)
            log.info("Dimensiones actualizadas")
            return True
        except Exception as e:
            log.error(f"Error cargando dimensiones: {e}")
            self.metrics["errors"] += 1
            return False

    def load_facts(self, df: pd.DataFrame):
        """Busca las claves de dimensión en Snowflake e inserta o actualiza los hechos."""
        log.info("Cargando tabla de hechos...")
        cols = ["delivery_id", "trip_id", "tracking_number", "customer_name",
                "vehicle_id", "driver_id", "route_id",
                "scheduled_datetime", "delivered_datetime",
                "package_weight_kg", "distance_km", "fuel_consumed_liters",
                "delivery_time_minutes", "delay_minutes", "deliveries_per_hour",
                "fuel_efficiency_km_per_liter", "cost_per_delivery", "revenue_per_delivery",
                "is_on_time", "is_damaged", "has_signature", "delivery_status"]
        try:
            self._upload(df[cols], "tmp_fact")
            metric_cols = ["package_weight_kg", "distance_km", "fuel_consumed_liters",
                           "delivery_time_minutes", "delay_minutes", "deliveries_per_hour",
                           "fuel_efficiency_km_per_liter", "cost_per_delivery",
                           "revenue_per_delivery", "is_on_time", "is_damaged",
                           "has_signature", "delivery_status"]
            key_cols = ["date_key", "scheduled_time_key", "delivered_time_key",
                        "vehicle_key", "driver_key", "route_key", "customer_key"]
            all_cols = key_cols + ["delivery_id", "trip_id", "tracking_number"] + metric_cols
            upd = ", ".join(f"{c} = s.{c}" for c in key_cols + metric_cols)
            res = self._sf_exec(f"""
                MERGE INTO fact_deliveries f
                USING (
                    SELECT
                        TO_NUMBER(TO_CHAR(t.delivered_datetime::TIMESTAMP_NTZ::DATE, 'YYYYMMDD')) AS date_key,
                        HOUR(t.scheduled_datetime::TIMESTAMP_NTZ) * 100
                            + MINUTE(t.scheduled_datetime::TIMESTAMP_NTZ)  AS scheduled_time_key,
                        HOUR(t.delivered_datetime::TIMESTAMP_NTZ) * 100
                            + MINUTE(t.delivered_datetime::TIMESTAMP_NTZ)  AS delivered_time_key,
                        v.vehicle_key, dr.driver_key, r.route_key, c.customer_key,
                        t.delivery_id, t.trip_id, t.tracking_number,
                        {", ".join("t." + m for m in metric_cols)}
                    FROM tmp_fact t
                    LEFT JOIN dim_vehicle v
                           ON v.vehicle_id = t.vehicle_id
                          AND t.delivered_datetime::TIMESTAMP_NTZ::DATE BETWEEN v.valid_from AND v.valid_to
                    LEFT JOIN dim_driver dr
                           ON dr.driver_id = t.driver_id
                          AND t.delivered_datetime::TIMESTAMP_NTZ::DATE BETWEEN dr.valid_from AND dr.valid_to
                    LEFT JOIN dim_route r     ON r.route_id = t.route_id
                    LEFT JOIN dim_customer c  ON c.customer_name = t.customer_name
                ) s
                ON f.delivery_id = s.delivery_id
                WHEN MATCHED THEN UPDATE SET {upd}, etl_batch_id = {self.batch_id},
                                             etl_timestamp = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT ({", ".join(all_cols)}, etl_batch_id)
                VALUES ({", ".join("s." + c for c in all_cols)}, {self.batch_id})
            """)
            inserted, updated = (res[0][0], res[0][1]) if res else (0, 0)
            self.metrics["facts_inserted"] = int(inserted)
            self.metrics["facts_updated"] = int(updated)
            self.metrics["records_loaded"] = int(inserted) + int(updated)
            log.info(f"fact_deliveries: {inserted:,} insertados, {updated:,} actualizados")

            # Control: hechos sin clave de dimensión
            missing = self._sf_exec(f"""
                SELECT COUNT(*) FROM fact_deliveries
                WHERE etl_batch_id = {self.batch_id}
                  AND (vehicle_key IS NULL OR driver_key IS NULL OR route_key IS NULL
                       OR customer_key IS NULL OR date_key IS NULL)
            """)[0][0]
            self.metrics["facts_without_keys"] = int(missing)
            if missing:
                log.warning(f"{missing} hechos sin clave de dimensión")
        except Exception as e:
            log.error(f"Error cargando hechos: {e}")
            self.metrics["errors"] += 1

    def _calculate_daily_totals(self):
        """Pre-calcula totales por día para reportes rápidos."""
        try:
            self._sf_exec("""
                CREATE TABLE IF NOT EXISTS daily_totals (
                    date_key INT PRIMARY KEY, total_deliveries INT, on_time_deliveries INT,
                    on_time_pct DECIMAL(5,2), avg_delay_minutes DECIMAL(10,2),
                    total_weight_kg DECIMAL(14,2), total_distance_km DECIMAL(14,2),
                    total_fuel_liters DECIMAL(14,2), total_cost DECIMAL(16,2),
                    total_revenue DECIMAL(16,2), active_drivers INT, active_vehicles INT,
                    etl_batch_id INT, updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP())
            """)
            start_key = int(self.start.strftime("%Y%m%d")) if self.start.year > 1900 else 0
            end_key = int(self.effective_date.strftime("%Y%m%d"))
            self._sf_exec(f"""
                MERGE INTO daily_totals t
                USING (
                    SELECT date_key,
                           COUNT(*)                                   AS total_deliveries,
                           COUNT_IF(is_on_time)                       AS on_time_deliveries,
                           ROUND(100 * COUNT_IF(is_on_time) / COUNT(*), 2) AS on_time_pct,
                           ROUND(AVG(delay_minutes), 2)               AS avg_delay_minutes,
                           SUM(package_weight_kg)                     AS total_weight_kg,
                           SUM(distance_km)                           AS total_distance_km,
                           SUM(fuel_consumed_liters)                  AS total_fuel_liters,
                           SUM(cost_per_delivery)                     AS total_cost,
                           SUM(revenue_per_delivery)                  AS total_revenue,
                           COUNT(DISTINCT driver_key)                 AS active_drivers,
                           COUNT(DISTINCT vehicle_key)                AS active_vehicles
                    FROM fact_deliveries
                    WHERE date_key BETWEEN {start_key} AND {end_key}
                    GROUP BY date_key
                ) s ON t.date_key = s.date_key
                WHEN MATCHED THEN UPDATE SET
                    total_deliveries = s.total_deliveries, on_time_deliveries = s.on_time_deliveries,
                    on_time_pct = s.on_time_pct, avg_delay_minutes = s.avg_delay_minutes,
                    total_weight_kg = s.total_weight_kg, total_distance_km = s.total_distance_km,
                    total_fuel_liters = s.total_fuel_liters, total_cost = s.total_cost,
                    total_revenue = s.total_revenue, active_drivers = s.active_drivers,
                    active_vehicles = s.active_vehicles, etl_batch_id = {self.batch_id},
                    updated_at = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT
                    (date_key, total_deliveries, on_time_deliveries, on_time_pct,
                     avg_delay_minutes, total_weight_kg, total_distance_km, total_fuel_liters,
                     total_cost, total_revenue, active_drivers, active_vehicles, etl_batch_id)
                VALUES (s.date_key, s.total_deliveries, s.on_time_deliveries, s.on_time_pct,
                        s.avg_delay_minutes, s.total_weight_kg, s.total_distance_km,
                        s.total_fuel_liters, s.total_cost, s.total_revenue,
                        s.active_drivers, s.active_vehicles, {self.batch_id})
            """)
            log.info("Totales diarios calculados")
        except Exception as e:
            log.error(f"Error calculando totales: {e}")
            self.metrics["errors"] += 1

    # -------------------------------------------------
    # ORQUESTACIÓN
    # -------------------------------------------------
    def run_etl(self) -> dict:
        t0 = datetime.now()
        log.info(f"Iniciando ETL ({self.label}) - Batch ID: {self.batch_id}")
        try:
            if not self.connect_databases():
                return self.metrics

            raw = self.extract_daily_data()
            dims = self.transform_dimensions(self.extract_dimensions())
            if self.load_dimensions(dims) and not raw.empty:
                self.load_staging(raw)
                df = self.transform_data(raw.copy())
                if not df.empty:
                    self.load_facts(df)
                    self._calculate_daily_totals()
            elif raw.empty:
                log.info("No hay entregas completadas en el período")
        except Exception as e:
            log.error(f"Error fatal en ETL: {e}")
            self.metrics["errors"] += 1
        finally:
            self.close_connections()

        self.metrics["duration_seconds"] = round((datetime.now() - t0).total_seconds(), 1)
        log.info(f"ETL completado en {self.metrics['duration_seconds']} segundos")
        log.info(f"Métricas: {json.dumps(self.metrics, ensure_ascii=False)}")
        return self.metrics


# =====================================================
# EJECUCIÓN
# =====================================================

def run_full(until: date) -> dict:
    """Carga histórica: todas las entregas completadas antes de `until`."""
    return FleetLogixETL(date(1900, 1, 1), until, f"carga histórica hasta {until}").run_etl()


def run_day(day: date) -> dict:
    """Carga de un día."""
    return FleetLogixETL(day, day + timedelta(days=1), f"carga diaria {day}").run_etl()


def job():
    """Tarea programada: procesa el día anterior."""
    run_day(date.today() - timedelta(days=1))


def main():
    p = argparse.ArgumentParser(description="Pipeline ETL FleetLogix")
    p.add_argument("--full", action="store_true", help="carga histórica inicial")
    p.add_argument("--until", type=date.fromisoformat, help="fin (exclusivo) de la carga histórica")
    p.add_argument("--date", type=date.fromisoformat, help="procesar un día puntual (AAAA-MM-DD)")
    p.add_argument("--schedule", action="store_true", help="ejecutar todos los días a las 02:00")
    a = p.parse_args()

    if a.full:
        run_full(a.until or date.today())
    elif a.date:
        run_day(a.date)
    elif a.schedule:
        import schedule
        schedule.every().day.at("02:00").do(job)
        log.info("ETL programado todos los días a las 02:00 (Ctrl+C para detener)")
        while True:
            schedule.run_pending()
            time.sleep(60)
    else:
        job()


if __name__ == "__main__":
    main()
