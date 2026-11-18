"""
FleetLogix - Funciones Lambda para AWS
3 funciones simples para procesamiento básico
"""

import json
import boto3
from datetime import datetime, timedelta
from decimal import Decimal

# Clientes AWS
dynamodb = boto3.resource('dynamodb')
sns = boto3.client('sns')

# =====================================================
# LAMBDA 1: Verificar si una entrega se completó
# =====================================================
def lambda_verificar_entrega(event, context):
    """
    Verifica si una entrega se completó comparando con DynamoDB
    """
    
    # Obtener datos del evento
    delivery_id = event.get('delivery_id')
    tracking_number = event.get('tracking_number')
    
    if not delivery_id:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'delivery_id es requerido'})
        }
    
    # Conectar a tabla DynamoDB
    table = dynamodb.Table('deliveries_status')
    
    try:
        # Buscar entrega
        response = table.get_item(
            Key={'delivery_id': delivery_id}
        )
        
        if 'Item' in response:
            item = response['Item']
            is_completed = item.get('status') == 'delivered'
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'delivery_id': delivery_id,
                    'tracking_number': item.get('tracking_number'),
                    'is_completed': is_completed,
                    'status': item.get('status'),
                    'delivered_datetime': str(item.get('delivered_datetime', ''))
                })
            }
        else:
            return {
                'statusCode': 404,
                'body': json.dumps({
                    'error': 'Entrega no encontrada',
                    'delivery_id': delivery_id
                })
            }
            
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }

# =====================================================
# LAMBDA 2: Calcular tiempo estimado de llegada (ETA)
# =====================================================
def lambda_calcular_eta(event, context):
    """
    Calcula ETA basado en ubicación actual y destino
    """
    
    # Obtener datos del evento
    vehicle_id = event.get('vehicle_id')
    current_location = event.get('current_location')  # {lat, lon}
    destination = event.get('destination')  # {lat, lon}
    current_speed_kmh = event.get('current_speed_kmh', 60)
    
    if not all([vehicle_id, current_location, destination]):
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Faltan parámetros requeridos'})
        }
    
    try:
        # Calcular distancia simple (Haversine simplificado)
        lat_diff = abs(destination['lat'] - current_location['lat'])
        lon_diff = abs(destination['lon'] - current_location['lon'])
        
        # Aproximación simple: 111 km por grado
        distance_km = ((lat_diff ** 2 + lon_diff ** 2) ** 0.5) * 111
        
        # Calcular tiempo
        if current_speed_kmh > 0:
            hours = distance_km / current_speed_kmh
            eta = datetime.now() + timedelta(hours=hours)
        else:
            eta = None
        
        # Guardar en DynamoDB
        table = dynamodb.Table('vehicle_tracking')
        table.put_item(
            Item={
                'vehicle_id': vehicle_id,
                'timestamp': datetime.now().isoformat(),
                'current_location': current_location,
                'destination': destination,
                'distance_remaining_km': Decimal(str(round(distance_km, 2))),
                'eta': eta.isoformat() if eta else None,
                'current_speed_kmh': Decimal(str(current_speed_kmh))
            }
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'vehicle_id': vehicle_id,
                'distance_remaining_km': round(distance_km, 2),
                'eta': eta.isoformat() if eta else 'No disponible',
                'estimated_minutes': round(hours * 60) if eta else None
            })
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }

# =====================================================
# LAMBDA 3: Enviar alerta si camión se desvía de ruta
# =====================================================
def lambda_alerta_desvio(event, context):
    """
    Detecta desvíos de ruta y envía alertas
    """
    
    # Obtener datos del evento
    vehicle_id = event.get('vehicle_id')
    current_location = event.get('current_location')  # {lat, lon}
    route_id = event.get('route_id')
    driver_id = event.get('driver_id')
    
    if not all([vehicle_id, current_location, route_id]):
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Faltan parámetros requeridos'})
        }
    
    try:
        # Obtener ruta esperada de DynamoDB
        table = dynamodb.Table('routes_waypoints')
        response = table.get_item(
            Key={'route_id': route_id}
        )
        
        if 'Item' not in response:
            return {
                'statusCode': 404,
                'body': json.dumps({'error': 'Ruta no encontrada'})
            }
        
        waypoints = response['Item'].get('waypoints', [])
        
        # Calcular distancia mínima a la ruta
        min_distance = float('inf')
        for waypoint in waypoints:
            lat_diff = abs(waypoint['lat'] - current_location['lat'])
            lon_diff = abs(waypoint['lon'] - current_location['lon'])
            distance = ((lat_diff ** 2 + lon_diff ** 2) ** 0.5) * 111  # km
            min_distance = min(min_distance, distance)
        
        # Umbral de desvío: 5 km
        DEVIATION_THRESHOLD_KM = 5
        is_deviated = min_distance > DEVIATION_THRESHOLD_KM
        
        if is_deviated:
            # Enviar alerta SNS
            message = {
                'vehicle_id': vehicle_id,
                'driver_id': driver_id,
                'route_id': route_id,
                'deviation_km': round(min_distance, 2),
                'current_location': current_location,
                'timestamp': datetime.now().isoformat(),
                'alert_type': 'ROUTE_DEVIATION'
            }
            
            sns.publish(
                TopicArn='arn:aws:sns:us-east-1:123456789012:fleetlogix-alerts',
                Message=json.dumps(message),
                Subject='Alerta: Desvío de Ruta Detectado'
            )
            
            # Guardar alerta en DynamoDB
            alerts_table = dynamodb.Table('alerts_history')
            alerts_table.put_item(Item=message)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'vehicle_id': vehicle_id,
                'is_deviated': is_deviated,
                'deviation_km': round(min_distance, 2),
                'alert_sent': is_deviated,
                'threshold_km': DEVIATION_THRESHOLD_KM
            })
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }

# =====================================================
# Configuración de procesamiento automático
# =====================================================
"""
CONFIGURACIÓN EN AWS:

1. Lambda 1 - Verificar Entrega:
   - Trigger: API Gateway POST /deliveries/verify
   - Ejecuta cada vez que app móvil marca entrega
   
2. Lambda 2 - Calcular ETA:
   - Trigger: EventBridge cada 5 minutos
   - Procesa todos los vehículos en ruta
   
3. Lambda 3 - Alertas Desvío:
   - Trigger: Kinesis Stream de GPS
   - Ejecuta con cada actualización de ubicación
"""