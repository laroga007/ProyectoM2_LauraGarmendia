"""
FleetLogix - Funciones Lambda (versión propuesta)

Versión ajustada de lambda_functions.py a partir del análisis del avance 4
(AWS_Análisis_Arquitectura.pdf, secciones 4 y 9). No se desplegó en AWS: se probó
con la maqueta local del notebook Avance_4_CloudArchitecture.ipynb.

Cada función se despliega como una Lambda distinta, indicando su handler:
    lambda_functions_propuesta.registrar_evento    <- API Gateway  POST /deliveries/event
    lambda_functions_propuesta.verificar_entrega   <- API Gateway  GET  /deliveries/{id}/status
    lambda_functions_propuesta.alerta_desvio       <- Kinesis (posiciones GPS)
    lambda_functions_propuesta.calcular_eta        <- EventBridge Scheduler, cada 5 minutos

Ajustes respecto de la versión de base:
    * Leen el evento en el formato de cada disparador (invocación directa, API Gateway,
      Kinesis en base64) en lugar de suponer campos sueltos.
    * Claves de DynamoDB como texto, igual que el esquema de las tablas (tipo S).
    * Números con Decimal: DynamoDB no acepta float de Python.
    * Desvío medido contra los tramos de la ruta, no solo contra sus puntos.
    * Una alerta por desvío (pausa configurable), no una por cada posición.
    * ETA con la distancia por ruta y la velocidad media de la ruta, no en línea recta a 60 km/h.
    * ARN del tema SNS y umbrales en variables de entorno, no escritos en el código.
    * Registro con logging (llega a CloudWatch) y respuestas JSON con Content-Type.
"""

import base64
import json
import logging
import math
import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import boto3

log = logging.getLogger()
log.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")

ALERTS_TOPIC_ARN = os.environ.get("ALERTS_TOPIC_ARN", "")
DESVIO_UMBRAL_KM = float(os.environ.get("DESVIO_UMBRAL_KM", "5"))
ALERTA_PAUSA_MIN = int(os.environ.get("ALERTA_PAUSA_MIN", "30"))
ESTADOS_VALIDOS = {"pending", "in_transit", "delivered", "failed"}
ULTIMA = "ULTIMA"          # clave de orden del registro con la última posición de cada vehículo


# =====================================================
# Utilidades
# =====================================================

def ahora():
    """Hora actual. Se puede reemplazar en las pruebas para simular otro momento."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def a_decimal(valor):
    """Convierte float (también dentro de listas y diccionarios) a Decimal para DynamoDB."""
    if isinstance(valor, float):
        return Decimal(str(round(valor, 6)))
    if isinstance(valor, dict):
        return {k: a_decimal(v) for k, v in valor.items()}
    if isinstance(valor, list):
        return [a_decimal(v) for v in valor]
    return valor


def de_decimal(valor):
    """Convierte Decimal a int o float para responder en JSON."""
    if isinstance(valor, Decimal):
        return int(valor) if valor == valor.to_integral_value() else float(valor)
    if isinstance(valor, dict):
        return {k: de_decimal(v) for k, v in valor.items()}
    if isinstance(valor, list):
        return [de_decimal(v) for v in valor]
    return valor


def respuesta(status, cuerpo):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(de_decimal(cuerpo), ensure_ascii=False),
    }


def leer_evento(event):
    """Une los datos del evento: invocación directa, body de API Gateway y parámetros de la URL."""
    datos = {k: v for k, v in event.items()
             if k not in ("body", "pathParameters", "queryStringParameters", "headers", "requestContext")}
    body = event.get("body")
    if isinstance(body, str) and body:
        datos.update(json.loads(body))
    elif isinstance(body, dict):
        datos.update(body)
    datos.update(event.get("queryStringParameters") or {})
    datos.update(event.get("pathParameters") or {})
    return datos


def haversine_km(a, b):
    """Distancia en km sobre la superficie terrestre entre dos puntos {lat, lon}."""
    lat1, lon1, lat2, lon2 = map(math.radians, (float(a["lat"]), float(a["lon"]),
                                                float(b["lat"]), float(b["lon"])))
    h = (math.sin((lat2 - lat1) / 2) ** 2
         + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2)
    return 2 * 6371 * math.asin(math.sqrt(h))


def _xy(p, lat0):
    """Proyección plana local en km (suficiente para distancias de pocos km)."""
    return (float(p["lon"]) * 111.32 * math.cos(math.radians(lat0)), float(p["lat"]) * 110.57)


def distancia_a_ruta(p, waypoints):
    """Distancia mínima (km) de p a la polilínea de la ruta y km recorridos hasta ese punto."""
    lat0 = float(p["lat"])
    px, py = _xy(p, lat0)
    mejor, km_hasta, acumulado = float("inf"), 0.0, 0.0
    for a, b in zip(waypoints, waypoints[1:]):
        ax, ay = _xy(a, lat0)
        bx, by = _xy(b, lat0)
        dx, dy = bx - ax, by - ay
        largo2 = dx * dx + dy * dy
        t = 0.0 if largo2 == 0 else max(0.0, min(1.0, ((px - ax) * dx + (py - ay) * dy) / largo2))
        d = math.hypot(px - (ax + t * dx), py - (ay + t * dy))
        if d < mejor:
            mejor, km_hasta = d, acumulado + t * math.sqrt(largo2)
        acumulado += math.sqrt(largo2)
    return mejor, km_hasta, acumulado


def insertar_en_rds(item):
    """Registro oficial en RDS. Se activa solo si la función tiene configurada la base
    (variable RDS_HOST); en la maqueta local queda desactivado."""
    if not os.environ.get("RDS_HOST"):
        return False
    import psycopg2  # capa (layer) de Lambda; autenticación IAM, sin contraseña en el código
    token = boto3.client("rds").generate_db_auth_token(
        os.environ["RDS_HOST"], 5432, os.environ["RDS_USER"])
    with psycopg2.connect(host=os.environ["RDS_HOST"], dbname="fleetlogix", user=os.environ["RDS_USER"],
                          password=token, sslmode="require") as conn, conn.cursor() as cur:
        cur.execute("UPDATE deliveries SET delivery_status = %s, delivered_datetime = %s WHERE delivery_id = %s",
                    (item["status"], item.get("delivered_datetime"), int(item["delivery_id"])))
    return True


# =====================================================
# 1. registrar-evento (nueva): guarda cada evento de entrega
# =====================================================

def registrar_evento(event, context=None):
    datos = leer_evento(event)
    delivery_id, estado = datos.get("delivery_id"), datos.get("status")
    if not delivery_id or estado not in ESTADOS_VALIDOS:
        return respuesta(400, {"error": "delivery_id y un status válido son requeridos",
                               "estados_validos": sorted(ESTADOS_VALIDOS)})
    item = {
        "delivery_id": str(delivery_id),
        "tracking_number": datos.get("tracking_number"),
        "trip_id": datos.get("trip_id"),
        "status": estado,
        "updated_at": ahora().isoformat(timespec="seconds"),
    }
    if estado == "delivered":
        item["delivered_datetime"] = datos.get("timestamp") or item["updated_at"]
    try:
        dynamodb.Table("deliveries_status").put_item(Item=a_decimal(item))
        en_rds = insertar_en_rds(item)
        log.info("evento registrado %s", json.dumps(item))
        return respuesta(200, {"registrado": True, "en_rds": en_rds, **item})
    except Exception as e:
        log.exception("error registrando evento")
        return respuesta(500, {"error": str(e)})


# =====================================================
# 2. verificar-entrega: ¿la entrega se completó?
# =====================================================

def verificar_entrega(event, context=None):
    datos = leer_evento(event)
    delivery_id = datos.get("delivery_id") or datos.get("id")
    if not delivery_id:
        return respuesta(400, {"error": "delivery_id es requerido"})
    delivery_id = str(delivery_id)           # la tabla define la clave como texto
    try:
        item = dynamodb.Table("deliveries_status").get_item(Key={"delivery_id": delivery_id}).get("Item")
        if not item:
            return respuesta(404, {"error": "Entrega no encontrada", "delivery_id": delivery_id})
        return respuesta(200, {
            "delivery_id": delivery_id,
            "tracking_number": item.get("tracking_number"),
            "is_completed": item.get("status") == "delivered",
            "status": item.get("status"),
            "delivered_datetime": item.get("delivered_datetime"),
        })
    except Exception as e:
        log.exception("error verificando entrega %s", delivery_id)
        return respuesta(500, {"error": str(e)})


# =====================================================
# 3. alerta-desvio: cada posición GPS que llega por Kinesis
# =====================================================

def _posiciones(event):
    """Kinesis entrega lotes de registros codificados en base64; también acepta una posición suelta."""
    if "Records" in event:
        for r in event["Records"]:
            yield json.loads(base64.b64decode(r["kinesis"]["data"]))
    else:
        yield leer_evento(event)


def alerta_desvio(event, context=None):
    rutas = dynamodb.Table("routes_waypoints")
    tracking = dynamodb.Table("vehicle_tracking")
    alertas = dynamodb.Table("alerts_history")
    resultados = []
    for pos in _posiciones(event):
        vehicle_id, route_id = str(pos["vehicle_id"]), str(pos["route_id"])
        punto = {"lat": float(pos["lat"]), "lon": float(pos["lon"])}
        momento = pos.get("timestamp") or ahora().isoformat(timespec="seconds")
        ruta = rutas.get_item(Key={"route_id": route_id}).get("Item")
        if not ruta:
            resultados.append({"vehicle_id": vehicle_id, "error": "ruta no encontrada"})
            continue
        desvio_km, km_recorridos, _ = distancia_a_ruta(punto, ruta["waypoints"])

        # última posición conocida del vehículo (la usa calcular-eta)
        tracking.put_item(Item=a_decimal({
            "vehicle_id": vehicle_id, "timestamp": ULTIMA, "route_id": route_id,
            "driver_id": str(pos.get("driver_id", "")), "position": punto,
            "position_time": momento, "deviation_km": desvio_km,
        }))

        alerta_enviada = False
        if desvio_km > DESVIO_UMBRAL_KM:
            previa = alertas.get_item(Key={"vehicle_id": vehicle_id, "timestamp": ULTIMA}).get("Item")
            reciente = previa and (datetime.fromisoformat(momento)
                                   - datetime.fromisoformat(previa["alert_time"])
                                   < timedelta(minutes=ALERTA_PAUSA_MIN))
            if not reciente:
                mensaje = {"alert_type": "ROUTE_DEVIATION", "vehicle_id": vehicle_id,
                           "driver_id": pos.get("driver_id"), "route_id": route_id,
                           "deviation_km": round(desvio_km, 2), "position": punto, "timestamp": momento}
                sns.publish(TopicArn=ALERTS_TOPIC_ARN, Message=json.dumps(mensaje),
                            Subject="FleetLogix: desvío de ruta")
                alertas.put_item(Item=a_decimal({**mensaje, "timestamp": momento}))
                alertas.put_item(Item={"vehicle_id": vehicle_id, "timestamp": ULTIMA, "alert_time": momento})
                alerta_enviada = True
        resultados.append({"vehicle_id": vehicle_id, "deviation_km": round(desvio_km, 2),
                           "is_deviated": desvio_km > DESVIO_UMBRAL_KM, "alert_sent": alerta_enviada})
    log.info("posiciones procesadas: %d", len(resultados))
    return {"procesadas": len(resultados), "resultados": resultados}


# =====================================================
# 4. calcular-eta: cada 5 minutos, para todos los vehículos en ruta
# =====================================================

def calcular_eta(event=None, context=None):
    rutas = dynamodb.Table("routes_waypoints")
    tracking = dynamodb.Table("vehicle_tracking")
    vehiculos = [i for i in tracking.scan().get("Items", []) if i["timestamp"] == ULTIMA]
    momento = ahora()
    resultados = []
    for v in vehiculos:
        ruta = rutas.get_item(Key={"route_id": v["route_id"]}).get("Item")
        if not ruta:
            continue
        _, km_recorridos, km_polilinea = distancia_a_ruta(v["position"], ruta["waypoints"])
        # los puntos de la ruta resumen el camino; se escala a la distancia real por carretera
        factor = float(ruta["distance_km"]) / km_polilinea
        km_restantes = max(0.0, (km_polilinea - km_recorridos) * factor)
        velocidad = float(ruta["distance_km"]) / float(ruta["estimated_duration_hours"])
        eta = momento + timedelta(hours=km_restantes / velocidad)
        registro = {"vehicle_id": v["vehicle_id"], "timestamp": momento.isoformat(timespec="seconds"),
                    "route_id": v["route_id"], "distance_remaining_km": round(km_restantes, 1),
                    "avg_speed_kmh": round(velocidad, 1), "eta": eta.isoformat(timespec="minutes")}
        tracking.put_item(Item=a_decimal(registro))
        resultados.append(registro)
    log.info("ETA calculado para %d vehículos", len(resultados))
    return {"vehiculos": len(resultados), "resultados": resultados}
