import json
import uuid
import pymysql
import sys
import rds_config

GET_FILLS_PATH = "/getLlenados"
GET_LATEST_FILL_PATH = "/getLatestLlenado"

rds_host = rds_config.db_endpoint
name = rds_config.db_username
password = rds_config.db_password
db_name = rds_config.db_name
port = 3306

try:
    conn = pymysql.connect(host=rds_host,
                           user=name,
                           passwd=password,
                           db=db_name,
                           connect_timeout=5)
except pymysql.MySQLError as e:
    sys.exit()
    

def lambda_handler(event, context):
    if event['rawPath'] == GET_FILLS_PATH:
        startInterval = event['queryStringParameters']['startDate']
        endInterval = event['queryStringParameters']['endDate']
        with conn.cursor() as qry_data:
            qry_fills = "SELECT llenados.id, llenados.fecha, llenados.porcentaje, llenados.presion, llenados.temperatura, cilindros.tipo, equipos.nombre, operadores.nombre \
                        FROM llenados, cilindros, equipos, operadores \
                        WHERE llenados.cilindro=cilindros.id \
                        AND llenados.equipo=equipos.id \
                        AND llenados.operador=operadores.id \
                        AND llenados.fecha \
                        BETWEEN '%s 00:00:01' AND '%s 23:59:59'" %(startInterval, endInterval)
            qry_data.execute(qry_fills)
            respose_fills = qry_data.fetchall()
            
        #confirmar y deshacer transacciones
        conn.commit()
        #cerrar conexion a base de datos
        conn.close
        
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
                'Access-Control-Allow-Credentials': 'true',
                'Content-Type': 'application/json'
            },
            'body': json.dumps(respose_fills, default=str)
        }
    elif event['rawPath'] == GET_LATEST_FILL_PATH:
        with conn.cursor() as qry_data:
            qry_latest_fill = "SELECT temperatura, presion, porcentaje FROM llenados WHERE id= (SELECT MAX(id) AS id FROM llenados)"
            qry_data.execute(qry_latest_fill)
            respose_latest_fills = qry_data.fetchall()
         
        #confirmar y deshacer transacciones
        conn.commit()
        #cerrar conexion a base de datos
        conn.close
        
        return {
            'statusCode': 200, 
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
                'Access-Control-Allow-Credentials': 'true',
                'Content-Type': 'application/json'
            },
            'body': json.dumps(respose_latest_fills, default=str)
        }