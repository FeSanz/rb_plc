import json
import uuid
import pymysql
import sys
import rds_config

rds_host = rds_config.db_endpoint
name = rds_config.db_username
password = rds_config.db_password
db_name = rds_config.db_name
port = 3306

def sql_response():
    startInterval = "2021-05-15"
    endInterval = "2021-08-24"
    with conn.cursor() as qry_data:
            qry_fills = "SELECT llenados.id, llenados.fecha, llenados.porcentaje, llenados.presion, llenados.temperatura, cilindros.tipo, equipos.nombre, operadores.nombre \
                        FROM llenados, cilindros, equipos, operadores \
                        WHERE llenados.cilindro=cilindros.id \
                        AND llenados.equipo=equipos.id \
                        AND llenados.operador=operadores.id \
                        AND llenados.fecha \
                        BETWEEN '%s 00:00:01' AND '%s 23:59:59'" %(startInterval, endInterval)
            #row_headers=[x[0] for x in qry_data.description] 
            qry_data.execute(qry_fills)
            respose_fills = qry_data.fetchall()

            json_data=[]
            for result in respose_fills:
                    content = {
                        'id': result[0], 
                        'fecha': result[1], 
                        'porcentaje': result[2], 
                        'presion': result[3], 
                        'temperatura': result[4], 
                        'cilindroTipo': result[5], 
                        'equipoNombre': result[6], 
                        'operadorNombre': result[7]
                        }
                    json_data.append(content)
            
    #confirmar y deshacer transacciones
    conn.commit()
    #cerrar conexion a base de datos
    conn.close

    json_struct = {
            'fillsInterval': json.dumps(json_data, default=str)
        }

    print(json_struct)
    
try:
    conn = pymysql.connect(host=rds_host,
                           user=name,
                           passwd=password,
                           db=db_name,
                           connect_timeout=5)
except pymysql.MySQLError as e:
    sys.exit()

sql_response()

