"""
FleetLogix - Generación de Datos Sintéticos (Avance 1)
=======================================================

Genera 505.650 registros para las 6 tablas del modelo relacional de FleetLogix,
respetando integridad referencial, consistencia temporal y reglas del negocio.

    Tablas maestras (650):        vehicles 200 | drivers 400 | routes 50
    Tablas transaccionales (505k): trips 100.000 | deliveries 400.000 | maintenance 5.000

Diseño general
--------------
1. GENERAR en memoria (pandas DataFrames), tabla por tabla y en orden de dependencias.
   Los IDs se asignan explícitamente (1..N), así cada FK apunta a un ID que
   sabemos que existe *antes* de tocar la base.
2. CARGAR todo en una única transacción con COPY (la vía más rápida de
   PostgreSQL para cargas masivas). Si algo falla, se hace ROLLBACK y la base
   queda como estaba: nunca queda "a medio cargar".
3. VALIDAR con SQL: integridad referencial, consistencia temporal, capacidad,
   solapamiento de horarios, cantidades y distribuciones. Todo queda en el log
   (data_generation.log) y en generation_summary.json.

Uso
---
    python 01_data_generation.py                 # genera y carga en PostgreSQL
    python 01_data_generation.py --csv-dir out   # solo genera CSVs (sin base)
    python 01_data_generation.py --validate-only # solo corre las validaciones

Conexión: se toma de variables de entorno (ver DB_CONFIG) o de los valores por
defecto. La contraseña se puede pasar con FLEETLOGIX_DB_PASSWORD.
"""

import argparse
import io
import json
import logging
import os
import random
import sys
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from faker import Faker

# =====================================================================
# CONFIGURACIÓN
# =====================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("data_generation.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("fleetlogix")

DB_CONFIG = {
    "host": os.getenv("FLEETLOGIX_DB_HOST", "localhost"),
    "database": os.getenv("FLEETLOGIX_DB_NAME", "fleetlogix"),
    "user": os.getenv("FLEETLOGIX_DB_USER", "postgres"),
    "password": os.getenv("FLEETLOGIX_DB_PASSWORD", "your_password"),  # Cambiar por tu contraseña
    "port": int(os.getenv("FLEETLOGIX_DB_PORT", "5432")),
}

SEED = 42

# Volúmenes pedidos por la consigna
N_VEHICLES = 200
N_DRIVERS = 400
N_ROUTES = 50
N_TRIPS = 100_000
N_DELIVERIES = 400_000
N_MAINTENANCE = 5_000
HISTORY_DAYS = 730  # 2 años de operación

CITIES = ["Bogotá", "Medellín", "Cali", "Barranquilla", "Cartagena"]

# Tipo de vehículo -> (capacidad_kg, combustible, proporción en flota,
#                      consumo L/100km (min, max), minutos planificados por parada,
#                      factor de costo de mantenimiento)
VEHICLE_TYPES = {
    "Camión Grande":  (5000, "diesel",   0.30, (28.0, 38.0), 60, 1.60),
    "Camión Mediano": (3000, "diesel",   0.30, (18.0, 26.0), 45, 1.20),
    "Van":            (1500, "gasolina", 0.30, (10.0, 14.0), 35, 1.00),
    "Motocicleta":    (50,   "gasolina", 0.10, (2.5, 4.0),   25, 0.35),
}

# Preferencia de tipo de vehículo según el largo de la ruta (pesos relativos).
# Una moto no hace Bogotá-Barranquilla (1.000 km): solo rutas cortas.
SHORT_ROUTE_KM = 250
VEHICLE_PREFERENCE = {
    "short": {"Camión Grande": 0.5, "Camión Mediano": 1.0, "Van": 2.0, "Motocicleta": 3.0},
    "long":  {"Camión Grande": 2.0, "Camión Mediano": 2.0, "Van": 1.0, "Motocicleta": 0.0},
}

# Mantenimientos: (tipo, costo base COP, días hasta el próximo)
MAINTENANCE_TYPES = [
    ("Cambio de aceite", 150_000, 30),
    ("Revisión de frenos", 250_000, 60),
    ("Cambio de llantas", 450_000, 90),
    ("Mantenimiento general", 350_000, 45),
    ("Revisión de motor", 500_000, 60),
    ("Alineación y balanceo", 180_000, 30),
]

# Distancias aproximadas por carretera entre las 5 ciudades (km)
CITY_DISTANCES = {
    ("Bogotá", "Medellín"): 440,
    ("Bogotá", "Cali"): 460,
    ("Bogotá", "Barranquilla"): 1000,
    ("Bogotá", "Cartagena"): 1050,
    ("Cali", "Medellín"): 420,
    ("Barranquilla", "Medellín"): 640,
    ("Cartagena", "Medellín"): 640,
    ("Barranquilla", "Cali"): 1100,
    ("Cali", "Cartagena"): 1100,
    ("Barranquilla", "Cartagena"): 120,
}

VEHICLE_TURNAROUND_MIN = 60   # carga/descarga entre viajes del mismo vehículo
DRIVER_REST_MIN = 8 * 60      # descanso mínimo entre viajes del mismo conductor


class DataGenerator:
    """Genera, carga y valida los datos sintéticos de FleetLogix."""

    def __init__(self, db_config, reference_date=None, seed=SEED):
        self.db_config = db_config
        self.connection = None
        self.cursor = None

        # Fecha de referencia ("hoy"). Todo el histórico se calcula hacia atrás.
        self.now = (reference_date or datetime.now()).replace(second=0, microsecond=0)
        self.today = self.now.replace(hour=0, minute=0)
        self.start_date = self.today - timedelta(days=HISTORY_DAYS)

        # Semillas: misma semilla -> mismos datos (reproducibilidad)
        self.fake = Faker("es_CO")
        Faker.seed(seed)
        random.seed(seed)
        self.rng = np.random.default_rng(seed)

        self.cities = CITIES
        self.tables = {}  # nombre_tabla -> DataFrame generado
        self.counters = {t: 0 for t in
                         ["vehicles", "drivers", "routes", "trips", "deliveries", "maintenance"]}

    # -----------------------------------------------------------------
    # Utilidades
    # -----------------------------------------------------------------
    def _to_min(self, ts):
        """datetime -> minutos desde start_date (aritmética rápida en numpy)."""
        return (pd.Timestamp(ts) - pd.Timestamp(self.start_date)).total_seconds() / 60.0

    def _from_min(self, minutes):
        """minutos (array) -> Series de timestamps redondeados al minuto."""
        return (pd.Timestamp(self.start_date)
                + pd.to_timedelta(np.round(np.asarray(minutes, dtype=float)), unit="min"))

    def _random_dates(self, start, end, size):
        """Fechas uniformes entre start y end (ambas datetime/date)."""
        start, end = pd.Timestamp(start), pd.Timestamp(end)
        span_days = (end - start).days
        offsets = self.rng.integers(0, span_days + 1, size=size)
        return (start + pd.to_timedelta(offsets, unit="D")).date

    # -----------------------------------------------------------------
    # Conexión
    # -----------------------------------------------------------------
    def connect(self):
        """Establecer conexión con la base de datos."""
        import psycopg2  # import diferido: el modo --csv-dir no necesita psycopg2
        try:
            self.connection = psycopg2.connect(**self.db_config)
            self.cursor = self.connection.cursor()
            log.info("Conexión exitosa a PostgreSQL (%s/%s)",
                     self.db_config["host"], self.db_config["database"])
            return True
        except Exception as e:
            log.error("Error al conectar: %s", e)
            return False

    # =================================================================
    # 1. TABLAS MAESTRAS
    # =================================================================
    def generate_vehicles(self, count=N_VEHICLES):
        """200 vehículos: Camión Grande / Mediano / Van (30% c/u) y Motocicleta (10%)."""
        log.info("Generando %d vehículos...", count)
        types = list(VEHICLE_TYPES)
        probs = np.array([VEHICLE_TYPES[t][2] for t in types])
        v_types = self.rng.choice(types, size=count, p=probs / probs.sum())

        # Placas colombianas únicas: autos ABC123, motos ABC12D
        plates, used = [], set()
        letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        for vt in v_types:
            while True:
                head = "".join(self.rng.choice(list(letters), 3))
                if vt == "Motocicleta":
                    plate = f"{head}{self.rng.integers(10, 100)}{self.rng.choice(list(letters))}"
                else:
                    plate = f"{head}{self.rng.integers(100, 1000)}"
                if plate not in used:
                    used.add(plate)
                    plates.append(plate)
                    break

        df = pd.DataFrame({
            "vehicle_id": np.arange(1, count + 1),
            "license_plate": plates,
            "vehicle_type": v_types,
            "capacity_kg": [float(VEHICLE_TYPES[t][0]) for t in v_types],
            "fuel_type": [VEHICLE_TYPES[t][1] for t in v_types],
            # Adquiridos entre hace 5 años y hace 1 mes
            "acquisition_date": self._random_dates(self.today - timedelta(days=5 * 365),
                                                   self.today - timedelta(days=30), count),
            # 90% activos, 10% en taller hoy
            "status": self.rng.choice(["active", "maintenance"], size=count, p=[0.9, 0.1]),
        })
        self.tables["vehicles"] = df
        self.counters["vehicles"] = len(df)
        log.info("  %d vehículos generados %s", len(df),
                 df["vehicle_type"].value_counts().to_dict())
        return df

    def generate_drivers(self, count=N_DRIVERS):
        """400 conductores con licencias únicas y fechas coherentes."""
        log.info("Generando %d conductores...", count)
        hire = pd.to_datetime(self._random_dates(self.today - timedelta(days=5 * 365),
                                                 self.today - timedelta(days=7), count))
        # Licencia: vence entre hace 1 mes y dentro de 3 años (algunas por vencer
        # o ya vencidas, para que la Query 2 tenga resultados). Siempre posterior
        # a la contratación + 6 meses (nadie se contrata con licencia por vencer).
        expiry = pd.to_datetime(self._random_dates(self.today - timedelta(days=30),
                                                   self.today + timedelta(days=3 * 365), count))
        expiry = np.maximum(expiry, hire + pd.Timedelta(days=180))

        license_numbers, used = [], set()
        while len(license_numbers) < count:
            n = str(self.rng.integers(1_000_000_000, 10_000_000_000))
            if n not in used:
                used.add(n)
                license_numbers.append(n)

        status = self.rng.choice(["active", "inactive"], size=count, p=[0.95, 0.05])
        df = pd.DataFrame({
            "driver_id": np.arange(1, count + 1),
            "employee_code": [f"EMP{i:04d}" for i in range(1, count + 1)],
            "first_name": [self.fake.first_name() for _ in range(count)],
            "last_name": [self.fake.last_name() for _ in range(count)],
            "license_number": license_numbers,
            "license_expiry": pd.DatetimeIndex(expiry).date,
            "phone": [f"3{self.rng.integers(100_000_000, 1_000_000_000)}" for _ in range(count)],
            "hire_date": pd.DatetimeIndex(hire).date,
            "status": status,
        })

        # Columna auxiliar (NO se carga en la base): último día trabajado de los
        # conductores inactivos. Sirve para que no tengan viajes después de irse.
        end = []
        for h, s in zip(hire, status):
            if s == "inactive":
                lo = h + pd.Timedelta(days=90)
                hi = pd.Timestamp(self.today - timedelta(days=1))
                end.append(lo + (hi - lo) * self.rng.random() if lo < hi else hi)
            else:
                end.append(pd.Timestamp("2100-01-01"))
        df["_end_date"] = pd.to_datetime(end)

        self.tables["drivers"] = df
        self.counters["drivers"] = len(df)
        log.info("  %d conductores generados (%d activos)", len(df), (status == "active").sum())
        return df

    def _get_distance(self, origin, destination):
        """Distancia aproximada por carretera entre dos ciudades colombianas."""
        return CITY_DISTANCES.get((origin, destination)) or CITY_DISTANCES[(destination, origin)]

    def generate_routes(self, count=N_ROUTES):
        """50 rutas entre las 5 ciudades.

        20 pares origen-destino. Rutas por par: 3 si involucra Bogotá (8 pares = 24),
        3 para Barranquilla<->Cartagena (corredor costero corto, 2 pares = 6) y
        2 para el resto (10 pares = 20). Total = 50.
        (La plantilla original producía solo 48 rutas.)
        """
        log.info("Generando %d rutas...", count)
        rows, code = [], 1
        coastal = {"Barranquilla", "Cartagena"}
        for origin in self.cities:
            for destination in self.cities:
                if origin == destination:
                    continue
                if "Bogotá" in (origin, destination) or {origin, destination} == coastal:
                    n_variants = 3
                else:
                    n_variants = 2
                base = self._get_distance(origin, destination)
                for _ in range(n_variants):
                    # Variantes de la misma ruta (vía principal, alterna, etc.)
                    distance = round(base * self.rng.uniform(0.92, 1.12), 2)
                    speed = self.rng.uniform(60, 80)  # km/h promedio
                    rows.append({
                        "route_id": code,
                        "route_code": f"R{code:03d}",
                        "origin_city": origin,
                        "destination_city": destination,
                        "distance_km": distance,
                        "estimated_duration_hours": round(distance / speed, 2),
                        "toll_cost": float(int(distance / 100) * 15_000),  # 15k COP c/100 km
                    })
                    code += 1
        df = pd.DataFrame(rows).head(count)
        # Factor de terreno (auxiliar, no se carga): rutas de montaña consumen más
        df["_terrain"] = self.rng.uniform(0.95, 1.20, size=len(df)).round(3)
        self.tables["routes"] = df
        self.counters["routes"] = len(df)
        log.info("  %d rutas generadas", len(df))
        return df

    # =================================================================
    # 2. TABLAS TRANSACCIONALES
    # =================================================================
    def _get_hourly_distribution(self):
        """Distribución de probabilidad de la HORA de salida de un viaje.

        Parte de un peso base de 0.02 para las 24 horas y lo sube en horario
        laboral: 0.06 de 6 a 20 h, 0.08 en el pico de la mañana (8-12 h) y 0.07
        en el de la tarde (14-18 h). Al dividir por la suma se convierte en un
        vector de probabilidades que suma 1, apto para np.random.choice.
        """
        probs = np.ones(24) * 0.02   # Base 2%
        probs[6:20] = 0.06           # 6am-8pm más actividad
        probs[8:12] = 0.08           # 8am-12pm pico mañana
        probs[14:18] = 0.07          # 2pm-6pm pico tarde
        return probs / probs.sum()

    def _get_daily_weights(self):
        """Peso relativo de cada día del histórico: tendencia + semana + estacionalidad."""
        days = pd.date_range(self.start_date, self.today - timedelta(days=1), freq="D")
        t = np.arange(len(days)) / 365.0
        trend = 1 + 0.15 * t                                    # crecimiento ~15% anual
        weekday = np.array([1.0, 1.0, 1.0, 1.0, 1.05, 0.70, 0.35])[days.dayofweek]
        month = np.array([0.85, 0.95, 1.0, 1.0, 1.0, 1.05,
                          1.05, 1.0, 1.0, 1.0, 1.10, 1.25])[days.month - 1]
        w = trend * weekday * month
        return days, w / w.sum()

    def generate_trips(self, count=N_TRIPS, total_deliveries=N_DELIVERIES):
        """Genera 100.000 viajes en 2 años, asignando vehículo y conductor sin solapamientos.

        Pasos:
        1. Reparte los viajes por día (tendencia, día de semana, estacionalidad)
           y les asigna una hora de salida con _get_hourly_distribution().
        2. Recorre los viajes en orden cronológico. Para cada uno elige una ruta y
           busca un vehículo y un conductor ELEGIBLES:
             - vehículo: ya adquirido, libre (su viaje anterior terminó + 1 h),
               apto para la distancia (motos solo en rutas cortas) y, si hoy está
               en taller, sin viajes en los últimos 10 días;
             - conductor: ya contratado, con licencia vigente, sin haber dejado la
               empresa y con 8 h de descanso desde su viaje anterior.
        3. Calcula cuántas entregas tendrá el viaje y sus horarios (programado y
           real), con retrasos por incidentes más probables en viernes/lunes y en
           horas pico. La llegada del viaje es después de la última entrega, por lo
           que arrival > departure y todas las entregas caen dentro del viaje.
        4. Combustible según tipo de vehículo, distancia, terreno y carga.
        5. Estado: 'completed' si ya llegó; 'in_progress' si sigue en ruta
           (sin arrival_datetime ni consumo final).
        """
        log.info("Generando %d viajes...", count)
        veh = self.tables["vehicles"]
        drv = self.tables["drivers"]
        rts = self.tables["routes"]

        # --- 1. Fechas y horas de salida --------------------------------
        days, day_w = self._get_daily_weights()
        per_day = self.rng.multinomial(count, day_w)
        day_idx = np.repeat(np.arange(len(days)), per_day)
        hours = self.rng.choice(24, size=count, p=self._get_hourly_distribution())
        minutes = self.rng.integers(0, 60, size=count)
        dep = np.sort(day_idx * 1440.0 + hours * 60 + minutes)  # minutos desde start_date

        # --- Cantidad de entregas por viaje (2-6, moda 4) ajustada a 400.000 exacto
        n_del = self.rng.choice([2, 3, 4, 5, 6], size=count, p=[0.1, 0.2, 0.4, 0.2, 0.1])
        diff = total_deliveries - int(n_del.sum())
        while diff != 0:
            i = self.rng.integers(0, count)
            if diff > 0 and n_del[i] < 6:
                n_del[i] += 1
                diff -= 1
            elif diff < 0 and n_del[i] > 2:
                n_del[i] -= 1
                diff += 1

        # --- Arrays de recursos (en minutos desde start_date) ------------
        v_type = veh["vehicle_type"].to_numpy()
        v_cap = veh["capacity_kg"].to_numpy(float)
        v_acq = np.array([self._to_min(d) for d in veh["acquisition_date"]])
        v_until = np.where(veh["status"].to_numpy() == "maintenance",
                           self._to_min(self.today - timedelta(days=10)), np.inf)
        v_free = np.full(len(veh), -np.inf)
        pref_short = np.array([VEHICLE_PREFERENCE["short"][t] for t in v_type])
        pref_long = np.array([VEHICLE_PREFERENCE["long"][t] for t in v_type])

        d_hire = np.array([self._to_min(d) for d in drv["hire_date"]])
        d_lic = np.array([self._to_min(d) + 1439 for d in drv["license_expiry"]])
        d_end = np.array([self._to_min(min(d, pd.Timestamp("2099-01-01")))
                          for d in drv["_end_date"]])
        d_free = np.full(len(drv), -np.inf)

        r_dist = rts["distance_km"].to_numpy(float)
        r_est = rts["estimated_duration_hours"].to_numpy(float) * 60
        r_terrain = rts["_terrain"].to_numpy(float)
        now_min = self._to_min(self.now)

        # --- Salidas -----------------------------------------------------
        out_v = np.empty(count, int)
        out_d = np.empty(count, int)
        out_r = np.empty(count, int)
        out_dep = np.empty(count)
        out_arr = np.empty(count)
        out_fuel = np.empty(count)
        out_w = np.empty(count)
        sched_all, deliv_all = [], []   # horarios de cada entrega (para generate_deliveries)
        pushed = 0

        for i in range(count):
            t = dep[i]
            r = self.rng.integers(0, len(rts))
            est = r_est[r]
            pref = pref_short if r_dist[r] <= SHORT_ROUTE_KM else pref_long

            # Restricciones estáticas (no dependen de la disponibilidad)
            v_static = (v_acq <= t) & (t <= v_until) & (pref > 0)
            d_static = (d_hire <= t) & (t <= d_end) & (t + est <= d_lic)

            v_ok = v_static & (v_free <= t)
            d_ok = d_static & (d_free <= t)
            if not v_ok.any() or not d_ok.any():
                # Nadie libre: se demora la salida hasta que se libere el primero
                pushed += 1
                if not v_ok.any():
                    t = max(t, v_free[v_static].min())
                if not d_ok.any():
                    t = max(t, d_free[d_static].min())
                v_ok = v_static & (v_free <= t)
                d_ok = d_static & (d_free <= t)

            vc = np.flatnonzero(v_ok)
            p = pref[vc] / pref[vc].sum()
            v = vc[self.rng.choice(len(vc), p=p)]
            d = self.rng.choice(np.flatnonzero(d_ok))

            # --- Horarios de entregas (programado vs real) ---------------
            n = n_del[i]
            slot = VEHICLE_TYPES[v_type[v]][4]
            sched = t + est + slot * np.arange(n)          # plan del despacho
            dow = int((self.start_date + timedelta(minutes=t)).weekday())
            hour = int((t % 1440) // 60)
            p_incident = 0.05 + (0.04 if dow == 4 else 0) + (0.03 if dow == 0 else 0) \
                + (0.03 if hour in (7, 8, 17, 18) else 0)
            if self.rng.random() < p_incident:              # tráfico, clima, bloqueo vial
                trip_delay = self.rng.uniform(45, 150)
            else:
                trip_delay = float(np.clip(self.rng.normal(0, 8), -20, 20))
            noise = np.clip(self.rng.normal(0, 7, n), -25, 25)
            customer_issue = (self.rng.random(n) < 0.02) * self.rng.uniform(40, 120, n)
            deliv = sched + trip_delay + noise + customer_issue
            deliv = np.maximum.accumulate(np.maximum(deliv, t + 0.5 * est))
            deliv = deliv + 5 * np.arange(n)                 # al menos 5 min entre paradas
            arrival = deliv[-1] + self.rng.uniform(10, 30)   # cierre del viaje

            # --- Carga y combustible --------------------------------------
            load = self.rng.uniform(0.4, 0.9)                # 40-90% de la capacidad
            lo, hi = VEHICLE_TYPES[v_type[v]][3]
            l_100 = self.rng.uniform(lo, hi) * r_terrain[r] * (1 + 0.2 * (load - 0.65))

            out_v[i], out_d[i], out_r[i] = v, d, r
            out_dep[i], out_arr[i] = t, arrival
            out_w[i] = v_cap[v] * load
            out_fuel[i] = r_dist[r] * l_100 / 100
            sched_all.append(sched)
            deliv_all.append(deliv)

            v_free[v] = arrival + VEHICLE_TURNAROUND_MIN
            d_free[d] = arrival + DRIVER_REST_MIN

        completed = out_arr <= now_min
        df = pd.DataFrame({
            "trip_id": np.arange(1, count + 1),
            "vehicle_id": veh["vehicle_id"].to_numpy()[out_v],
            "driver_id": drv["driver_id"].to_numpy()[out_d],
            "route_id": rts["route_id"].to_numpy()[out_r],
            "departure_datetime": self._from_min(out_dep),
            "arrival_datetime": self._from_min(out_arr).where(completed),
            "fuel_consumed_liters": np.where(completed, np.round(out_fuel, 2), np.nan),
            "total_weight_kg": np.round(out_w, 2),
            "status": np.where(completed, "completed", "in_progress"),
        })
        self.tables["trips"] = df
        self.counters["trips"] = len(df)
        self._delivery_plan = {"n": n_del, "sched": sched_all, "deliv": deliv_all}
        log.info("  %d viajes generados | completed=%d in_progress=%d | salidas demoradas por falta de recursos=%d",
                 len(df), completed.sum(), (~completed).sum(), pushed)
        return df

    def _distribute_weight(self, total_weights, n_per_trip):
        """Reparte el peso de cada viaje entre sus paquetes (vectorizado).

        Cada paquete pesa al menos 0.5 kg y entre todos suman el 95% del peso del
        viaje (el 5% restante es embalaje/pallets). Garantiza sum(paquetes) <= viaje.
        """
        trip_of = np.repeat(np.arange(len(n_per_trip)), n_per_trip)
        g = self.rng.gamma(1.5, size=len(trip_of))
        share = g / np.bincount(trip_of, weights=g)[trip_of]
        usable = total_weights * 0.95 - 0.5 * n_per_trip
        return 0.5 + share * usable[trip_of]

    def _generate_customers(self, per_city=1600):
        """Cartera de clientes por ciudad (personas y empresas) con dirección fija.

        Las entregas se asignan con una distribución tipo Zipf: pocos clientes
        frecuentes concentran muchas entregas (patrón real de una cartera de clientes).
        """
        suffixes = ["S.A.S.", "Ltda.", "& Cía.", "S.A."]
        prefixes = ["Distribuciones", "Comercializadora", "Inversiones", "Almacenes", "Ferretería"]
        customers = {}
        for city in self.cities:
            names, addresses = [], []
            for _ in range(per_city):
                if self.rng.random() < 0.2:
                    names.append(f"{self.rng.choice(prefixes)} {self.fake.last_name()} "
                                 f"{self.rng.choice(suffixes)}")
                else:
                    names.append(self.fake.name())
                addresses.append(f"{self.fake.street_address()}, {city}")
            w = 1.0 / np.arange(1, per_city + 1) ** 0.8
            customers[city] = (np.array(names), np.array(addresses), w / w.sum())
        return customers

    def generate_deliveries(self, count=N_DELIVERIES):
        """400.000 entregas (2 a 6 por viaje, moda 4) en la ciudad destino del viaje."""
        log.info("Generando %d entregas...", count)
        trips = self.tables["trips"]
        routes = self.tables["routes"].set_index("route_id")
        plan = self._delivery_plan
        n = plan["n"]
        assert n.sum() == count, "La suma de entregas por viaje no coincide"

        trip_of = np.repeat(np.arange(len(trips)), n)
        seq = np.concatenate([np.arange(k) for k in n])
        sched_min = np.concatenate(plan["sched"])
        deliv_min = np.concatenate(plan["deliv"])
        now_min = self._to_min(self.now)

        # Clientes de la ciudad destino
        dest_city = routes.loc[trips["route_id"], "destination_city"].to_numpy()[trip_of]
        customers = self._generate_customers()
        cust_name = np.empty(count, dtype=object)
        cust_addr = np.empty(count, dtype=object)
        for city, (names, addrs, p) in customers.items():
            mask = dest_city == city
            idx = self.rng.choice(len(names), size=mask.sum(), p=p)
            cust_name[mask] = names[idx]
            cust_addr[mask] = addrs[idx]

        weights = self._distribute_weight(trips["total_weight_kg"].to_numpy(float), n)

        delivered = deliv_min <= now_min
        scheduled_ts = self._from_min(sched_min)
        df = pd.DataFrame({
            "delivery_id": np.arange(1, count + 1),
            "trip_id": trips["trip_id"].to_numpy()[trip_of],
            "tracking_number": [f"FL{y}{i:08d}" for y, i in
                                zip(scheduled_ts.year, range(1, count + 1))],
            "customer_name": cust_name,
            "delivery_address": cust_addr,
            "package_weight_kg": np.floor(weights * 100) / 100,  # truncar: nunca supera el viaje
            "scheduled_datetime": scheduled_ts,
            "delivered_datetime": self._from_min(deliv_min).where(delivered),
            "delivery_status": np.where(delivered, "delivered", "pending"),
            "recipient_signature": delivered & (self.rng.random(count) < 0.95),
        })
        df["_seq"] = seq  # auxiliar
        self.tables["deliveries"] = df
        self.counters["deliveries"] = len(df)
        log.info("  %d entregas generadas | delivered=%d pending=%d",
                 len(df), delivered.sum(), (~delivered).sum())
        return df

    def generate_maintenance(self, count=N_MAINTENANCE):
        """5.000 mantenimientos, ~1 cada 20 viajes de cada vehículo.

        Los 5.000 registros se reparten entre los vehículos proporcionalmente a
        sus viajes (método del mayor resto) y cada mantenimiento se ubica en la
        fecha en que el vehículo completó su viaje N.º 20, 40, 60... Los
        vehículos que hoy están en taller ('maintenance') tienen su último
        registro en los últimos días.
        """
        log.info("Generando %d registros de mantenimiento...", count)
        veh = self.tables["vehicles"].set_index("vehicle_id")
        trips = self.tables["trips"]
        routes = self.tables["routes"].set_index("route_id")
        done = trips[trips["status"] == "completed"].copy()
        done["km"] = routes.loc[done["route_id"], "distance_km"].to_numpy()
        done = done.sort_values("arrival_datetime")

        # Cuota por vehículo proporcional a sus viajes (suma exacta = count)
        trips_per_v = done.groupby("vehicle_id").size()
        in_shop = veh.index[veh["status"] == "maintenance"]
        trips_per_v = trips_per_v.reindex(veh.index, fill_value=0)
        raw = trips_per_v / trips_per_v.sum() * count
        quota = np.floor(raw).astype(int)
        rest = count - quota.sum()
        quota[(raw - quota).sort_values(ascending=False).index[:rest]] += 1

        technicians = [f"{self.fake.first_name()} {self.fake.last_name()}" for _ in range(30)]
        workshops = ["Taller Central", "Diesel Andino", "Servitaller Norte",
                     "Autocentro Costa", "Taller Pacífico"]
        rows = []
        for vid, k in quota.items():
            if k == 0:
                continue
            vt = done[done["vehicle_id"] == vid]
            plate, vtype = veh.loc[vid, "license_plate"], veh.loc[vid, "vehicle_type"]
            cost_factor = VEHICLE_TYPES[vtype][5]
            cum_km = vt["km"].cumsum().to_numpy()
            positions = ((np.arange(1, k + 1) * len(vt)) // (k + 1)).clip(0, len(vt) - 1)
            for j, pos in enumerate(positions):
                is_last_in_shop = vid in in_shop and j == k - 1
                if is_last_in_shop:
                    m_date = (self.today - timedelta(days=int(self.rng.integers(0, 6)))).date()
                    km = cum_km[-1]
                else:
                    m_date = (vt["arrival_datetime"].iloc[pos]
                              + pd.Timedelta(days=int(self.rng.integers(0, 2)))).date()
                    m_date = min(m_date, self.today.date())
                    km = cum_km[pos]
                m_type, base, days_next = MAINTENANCE_TYPES[self.rng.integers(len(MAINTENANCE_TYPES))]
                if is_last_in_shop:
                    m_type, base, days_next = MAINTENANCE_TYPES[4]  # Revisión de motor
                cost = base * cost_factor * self.rng.uniform(0.8, 1.2)
                rows.append({
                    "vehicle_id": vid,
                    "maintenance_date": m_date,
                    "maintenance_type": m_type,
                    "description": f"{m_type} - vehículo {plate} ({vtype}) - "
                                   f"odómetro operativo aprox. {km:,.0f} km",
                    "cost": round(cost, 2),
                    "next_maintenance_date": m_date + timedelta(days=days_next),
                    "performed_by": f"{self.rng.choice(technicians)} ({self.rng.choice(workshops)})",
                })

        df = (pd.DataFrame(rows).sort_values(["maintenance_date", "vehicle_id"])
              .reset_index(drop=True))
        df.insert(0, "maintenance_id", np.arange(1, len(df) + 1))
        self.tables["maintenance"] = df
        self.counters["maintenance"] = len(df)
        log.info("  %d mantenimientos generados (%d vehículos)", len(df), df["vehicle_id"].nunique())
        return df

    # =================================================================
    # 3. CARGA
    # =================================================================
    TABLE_ORDER = ["vehicles", "drivers", "routes", "trips", "deliveries", "maintenance"]
    ID_COLUMN = {"vehicles": "vehicle_id", "drivers": "driver_id", "routes": "route_id",
                 "trips": "trip_id", "deliveries": "delivery_id",
                 "maintenance": "maintenance_id"}

    def _db_frame(self, table):
        """DataFrame listo para cargar: sin columnas auxiliares (las que empiezan con _)."""
        df = self.tables[table]
        return df[[c for c in df.columns if not c.startswith("_")]]

    def export_csv(self, out_dir):
        """Exporta cada tabla a CSV (modo sin base de datos)."""
        os.makedirs(out_dir, exist_ok=True)
        for t in self.TABLE_ORDER:
            path = os.path.join(out_dir, f"{t}.csv")
            self._db_frame(t).to_csv(path, index=False)
            log.info("  CSV escrito: %s", path)

    def load_all(self, reset=True):
        """Carga las 6 tablas con COPY en UNA transacción (todo o nada)."""
        log.info("Cargando datos en PostgreSQL con COPY...")
        try:
            if reset:
                self.cursor.execute(
                    "TRUNCATE maintenance, deliveries, trips, routes, drivers, vehicles "
                    "RESTART IDENTITY CASCADE")
            for t in self.TABLE_ORDER:
                df = self._db_frame(t)
                buf = io.StringIO()
                df.to_csv(buf, index=False, header=False, na_rep="\\N")
                buf.seek(0)
                cols = ", ".join(df.columns)
                self.cursor.copy_expert(
                    f"COPY {t} ({cols}) FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf)
                # Como cargamos los IDs explícitos, se sincroniza la secuencia SERIAL
                idc = self.ID_COLUMN[t]
                self.cursor.execute(
                    f"SELECT setval(pg_get_serial_sequence('{t}', '{idc}'), "
                    f"(SELECT MAX({idc}) FROM {t}))")
                log.info("  %-12s %9s registros cargados", t, f"{len(df):,}")
            self.connection.commit()
            self.cursor.execute("ANALYZE")
            log.info("Carga confirmada (COMMIT)")
        except Exception:
            self.connection.rollback()
            log.exception("Error durante la carga: ROLLBACK, la base queda sin cambios")
            raise

    # =================================================================
    # 4. CONTROL DE CALIDAD
    # =================================================================
    def run_validations(self):
        """Ejecuta los chequeos de VALIDATIONS. Devuelve (ok_global, resultados)."""
        log.info("VALIDANDO CALIDAD DE DATOS...")
        results, all_ok = [], True
        for name, query in VALIDATIONS.items():
            self.cursor.execute(query)
            value = self.cursor.fetchone()[0]
            ok = value == 0
            all_ok &= ok
            results.append({"check": name, "violations": int(value), "ok": ok})
            (log.info if ok else log.warning)("  [%s] %s: %s", "OK" if ok else "FALLA", name, value)
        return all_ok, results

    def run_distribution_checks(self):
        """Estadísticas para verificar que las distribuciones son realistas."""
        log.info("DISTRIBUCIONES...")
        out = {}
        for name, query in DISTRIBUTION_QUERIES.items():
            df = pd.read_sql(query, self.connection)
            out[name] = df.to_dict(orient="records")
            log.info("  %s:\n%s", name, df.to_string(index=False))
        return out

    def generate_summary_report(self):
        """Conteos finales + validaciones + distribuciones -> generation_summary.json."""
        log.info("RESUMEN DE GENERACIÓN DE DATOS")
        log.info("=" * 50)
        counts, total = {}, 0
        for t in self.TABLE_ORDER:
            self.cursor.execute(f"SELECT COUNT(*) FROM {t}")
            counts[t] = self.cursor.fetchone()[0]
            total += counts[t]
            log.info("  %-12s %9s registros", t, f"{counts[t]:,}")
        log.info("  %-12s %9s registros", "TOTAL", f"{total:,}")

        ok, checks = self.run_validations()
        dist = self.run_distribution_checks()
        summary = {
            "generation_date": datetime.now().isoformat(timespec="seconds"),
            "reference_date": self.now.isoformat(),
            "seed": SEED,
            "total_records": total,
            "table_counts": counts,
            "validations_passed": bool(ok),
            "validations": checks,
            "distributions": dist,
        }
        with open("generation_summary.json", "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False, default=str)
        log.info("Resumen guardado en generation_summary.json")
        return summary

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        log.info("Conexión cerrada")

    # -----------------------------------------------------------------
    def generate_all(self):
        """Genera las 6 tablas en orden de dependencias (FK)."""
        self.generate_vehicles()
        self.generate_drivers()
        self.generate_routes()
        self.generate_trips()
        self.generate_deliveries()
        self.generate_maintenance()
        total = sum(len(self._db_frame(t)) for t in self.TABLE_ORDER)
        log.info("TOTAL generado en memoria: %s registros", f"{total:,}")


# =====================================================================
# CHEQUEOS DE CALIDAD (cada query devuelve la cantidad de violaciones; 0 = OK)
# =====================================================================
VALIDATIONS = {
    # --- Volúmenes exactos
    "Volumen - vehicles != 200": "SELECT ABS(COUNT(*) - 200) FROM vehicles",
    "Volumen - drivers != 400": "SELECT ABS(COUNT(*) - 400) FROM drivers",
    "Volumen - routes != 50": "SELECT ABS(COUNT(*) - 50) FROM routes",
    "Volumen - trips != 100000": "SELECT ABS(COUNT(*) - 100000) FROM trips",
    "Volumen - deliveries != 400000": "SELECT ABS(COUNT(*) - 400000) FROM deliveries",
    "Volumen - maintenance != 5000": "SELECT ABS(COUNT(*) - 5000) FROM maintenance",
    # --- Integridad referencial
    "Integridad - trips sin vehículo válido": """
        SELECT COUNT(*) FROM trips t LEFT JOIN vehicles v ON t.vehicle_id = v.vehicle_id
        WHERE v.vehicle_id IS NULL""",
    "Integridad - trips sin conductor válido": """
        SELECT COUNT(*) FROM trips t LEFT JOIN drivers d ON t.driver_id = d.driver_id
        WHERE d.driver_id IS NULL""",
    "Integridad - trips sin ruta válida": """
        SELECT COUNT(*) FROM trips t LEFT JOIN routes r ON t.route_id = r.route_id
        WHERE r.route_id IS NULL""",
    "Integridad - deliveries sin trip válido": """
        SELECT COUNT(*) FROM deliveries d LEFT JOIN trips t ON d.trip_id = t.trip_id
        WHERE t.trip_id IS NULL""",
    "Integridad - maintenance sin vehículo válido": """
        SELECT COUNT(*) FROM maintenance m LEFT JOIN vehicles v ON m.vehicle_id = v.vehicle_id
        WHERE v.vehicle_id IS NULL""",
    # --- Consistencia temporal
    "Temporal - trips con arrival <= departure": """
        SELECT COUNT(*) FROM trips
        WHERE arrival_datetime IS NOT NULL AND arrival_datetime <= departure_datetime""",
    "Temporal - trips en el futuro": "SELECT COUNT(*) FROM trips WHERE departure_datetime > NOW()",
    "Temporal - status incoherente con arrival": """
        SELECT COUNT(*) FROM trips
        WHERE (status = 'completed' AND arrival_datetime IS NULL)
           OR (status = 'in_progress' AND arrival_datetime IS NOT NULL)""",
    "Temporal - entregas fuera del intervalo del viaje": """
        SELECT COUNT(*) FROM deliveries d JOIN trips t ON d.trip_id = t.trip_id
        WHERE d.delivered_datetime < t.departure_datetime
           OR d.scheduled_datetime < t.departure_datetime
           OR (t.arrival_datetime IS NOT NULL AND d.delivered_datetime > t.arrival_datetime)""",
    "Temporal - delivered sin fecha / pending con fecha": """
        SELECT COUNT(*) FROM deliveries
        WHERE (delivery_status = 'delivered' AND delivered_datetime IS NULL)
           OR (delivery_status = 'pending' AND delivered_datetime IS NOT NULL)""",
    "Temporal - viaje antes de la contratación del conductor": """
        SELECT COUNT(*) FROM trips t JOIN drivers d ON t.driver_id = d.driver_id
        WHERE t.departure_datetime::date < d.hire_date""",
    "Temporal - viaje con licencia vencida": """
        SELECT COUNT(*) FROM trips t JOIN drivers d ON t.driver_id = d.driver_id
        WHERE t.departure_datetime::date > d.license_expiry""",
    "Temporal - viaje antes de adquirir el vehículo": """
        SELECT COUNT(*) FROM trips t JOIN vehicles v ON t.vehicle_id = v.vehicle_id
        WHERE t.departure_datetime::date < v.acquisition_date""",
    "Temporal - mantenimiento fuera de rango": """
        SELECT COUNT(*) FROM maintenance m JOIN vehicles v ON m.vehicle_id = v.vehicle_id
        WHERE m.maintenance_date < v.acquisition_date OR m.maintenance_date > CURRENT_DATE
           OR m.next_maintenance_date <= m.maintenance_date""",
    # --- Horarios (sin solapamientos)
    "Horarios - conductor con viajes superpuestos": """
        SELECT COUNT(*) FROM (
            SELECT departure_datetime,
                   LAG(arrival_datetime) OVER (PARTITION BY driver_id ORDER BY departure_datetime) AS prev_arr
            FROM trips) x
        WHERE departure_datetime < prev_arr""",
    "Horarios - vehículo con viajes superpuestos": """
        SELECT COUNT(*) FROM (
            SELECT departure_datetime,
                   LAG(arrival_datetime) OVER (PARTITION BY vehicle_id ORDER BY departure_datetime) AS prev_arr
            FROM trips) x
        WHERE departure_datetime < prev_arr""",
    # --- Capacidad y pesos
    "Capacidad - trips que exceden capacidad del vehículo": """
        SELECT COUNT(*) FROM trips t JOIN vehicles v ON t.vehicle_id = v.vehicle_id
        WHERE t.total_weight_kg > v.capacity_kg""",
    "Capacidad - paquetes que suman más que el viaje": """
        SELECT COUNT(*) FROM (
            SELECT t.trip_id FROM trips t JOIN deliveries d ON d.trip_id = t.trip_id
            GROUP BY t.trip_id, t.total_weight_kg
            HAVING SUM(d.package_weight_kg) > t.total_weight_kg) x""",
    "Capacidad - motocicletas en rutas largas": """
        SELECT COUNT(*) FROM trips t JOIN vehicles v ON t.vehicle_id = v.vehicle_id
        JOIN routes r ON t.route_id = r.route_id
        WHERE v.vehicle_type = 'Motocicleta' AND r.distance_km > 250""",
    # --- Reglas de negocio
    "Negocio - viajes con menos de 2 o más de 6 entregas": """
        SELECT COUNT(*) FROM (
            SELECT t.trip_id, COUNT(d.delivery_id) AS n FROM trips t
            LEFT JOIN deliveries d ON d.trip_id = t.trip_id GROUP BY t.trip_id) x
        WHERE n NOT BETWEEN 2 AND 6""",
    "Negocio - entregas en ciudad distinta al destino": """
        SELECT COUNT(*) FROM deliveries d JOIN trips t ON d.trip_id = t.trip_id
        JOIN routes r ON t.route_id = r.route_id
        WHERE d.delivery_address NOT LIKE '%, ' || r.destination_city""",
    "Negocio - firma sin entrega": """
        SELECT COUNT(*) FROM deliveries
        WHERE recipient_signature AND delivery_status <> 'delivered'""",
    "Negocio - origen igual a destino": "SELECT COUNT(*) FROM routes WHERE origin_city = destination_city",
    "Negocio - valores no positivos": """
        SELECT (SELECT COUNT(*) FROM trips WHERE fuel_consumed_liters <= 0 OR total_weight_kg <= 0)
             + (SELECT COUNT(*) FROM deliveries WHERE package_weight_kg <= 0)
             + (SELECT COUNT(*) FROM maintenance WHERE cost <= 0)
             + (SELECT COUNT(*) FROM routes WHERE distance_km <= 0)""",
    "Negocio - tracking number vacío": """
        SELECT COUNT(*) FROM deliveries WHERE tracking_number IS NULL OR tracking_number = ''""",
}

DISTRIBUTION_QUERIES = {
    "Entregas por viaje": """
        SELECT n AS entregas_por_viaje, COUNT(*) AS viajes,
               ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
        FROM (SELECT trip_id, COUNT(*) n FROM deliveries GROUP BY trip_id) x
        GROUP BY n ORDER BY n""",
    "Flota por tipo": """
        SELECT v.vehicle_type, COUNT(DISTINCT v.vehicle_id) AS vehiculos,
               COUNT(t.trip_id) AS viajes,
               ROUND(AVG(t.fuel_consumed_liters / r.distance_km * 100), 1) AS l_100km
        FROM vehicles v LEFT JOIN trips t ON t.vehicle_id = v.vehicle_id
        LEFT JOIN routes r ON r.route_id = t.route_id
        GROUP BY v.vehicle_type ORDER BY vehiculos DESC""",
    "Estados": """
        SELECT 'trips' AS tabla, status AS estado, COUNT(*) FROM trips GROUP BY status
        UNION ALL
        SELECT 'deliveries', delivery_status, COUNT(*) FROM deliveries GROUP BY delivery_status
        UNION ALL
        SELECT 'vehicles', status, COUNT(*) FROM vehicles GROUP BY status
        UNION ALL
        SELECT 'drivers', status, COUNT(*) FROM drivers GROUP BY status""",
    "Puntualidad (retraso > 30 min)": """
        SELECT ROUND(100.0 * AVG((delivered_datetime > scheduled_datetime + INTERVAL '30 minutes')::int), 1)
               AS pct_retrasadas,
               ROUND(AVG(EXTRACT(EPOCH FROM delivered_datetime - scheduled_datetime) / 60), 1)
               AS min_promedio_diferencia
        FROM deliveries WHERE delivery_status = 'delivered'""",
    "Viajes por mes (primeros/últimos)": """
        (SELECT TO_CHAR(DATE_TRUNC('month', departure_datetime), 'YYYY-MM') AS mes, COUNT(*) AS viajes
         FROM trips GROUP BY 1 ORDER BY 1 LIMIT 3)
        UNION ALL
        (SELECT TO_CHAR(DATE_TRUNC('month', departure_datetime), 'YYYY-MM'), COUNT(*)
         FROM trips GROUP BY 1 ORDER BY 1 DESC LIMIT 3)""",
    "Mantenimiento por vehículo": """
        SELECT MIN(n) AS min, ROUND(AVG(n), 1) AS promedio, MAX(n) AS max,
               ROUND(100000.0 / 5000, 1) AS viajes_por_mantenimiento_objetivo,
               ROUND((SELECT COUNT(*) FROM trips WHERE status='completed')::numeric
                     / (SELECT COUNT(*) FROM maintenance), 1) AS viajes_por_mantenimiento_real
        FROM (SELECT vehicle_id, COUNT(*) n FROM maintenance GROUP BY vehicle_id) x""",
}


def main():
    parser = argparse.ArgumentParser(description="FleetLogix - generación de datos sintéticos")
    parser.add_argument("--csv-dir", help="Solo generar CSVs en esta carpeta (no usa la base)")
    parser.add_argument("--validate-only", action="store_true", help="Solo correr validaciones")
    parser.add_argument("--reference-date", help="Fecha 'hoy' (YYYY-MM-DD HH:MM). Por defecto: ahora")
    args = parser.parse_args()

    print("FLEETLOGIX - Generación de Datos Masivos")
    print("=" * 60)
    print("Objetivo: Generar 505.650 registros manteniendo integridad")
    print("=" * 60)

    ref = datetime.strptime(args.reference_date, "%Y-%m-%d %H:%M") if args.reference_date else None
    generator = DataGenerator(DB_CONFIG, reference_date=ref)

    if args.validate_only:
        if generator.connect():
            generator.generate_summary_report()
            generator.close()
        return

    start = datetime.now()
    generator.generate_all()
    log.info("Generación en memoria: %.1f s", (datetime.now() - start).total_seconds())

    if args.csv_dir:
        generator.export_csv(args.csv_dir)
        return

    if not generator.connect():
        return
    try:
        generator.load_all(reset=True)
        generator.generate_summary_report()
        log.info("Proceso completo en %.1f s", (datetime.now() - start).total_seconds())
    finally:
        generator.close()


if __name__ == "__main__":
    main()
