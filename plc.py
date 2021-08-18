import mariadb
import time
import sys
from datetime import datetime
from pymodbus.client.sync import ModbusTcpClient as ModbusClient

UNIT = 0x1

#variables temporales para validacion de cambios
temp_presion = 0
temp_temperatura = 0
temp_porcentaje = 0
temp_ciclos = 0

#conexión a modbus con ip-puerto
client = ModbusClient('192.168.1.200', port=502)

# conexión a la base de datos MariaDB
try:
   conn = mariadb.connect(host="localhost", user="spaceuser", password="condor", database="infra")
except mariadb.Error as e:
   print(f"Error al conetarse a la base de datos: {e}")
   sys.exit(1)

while 1:
    #conexion modbus
    client.connect()
    #obtener arreglo de datos del PLC [1er equipo]
    rh = client.read_holding_registers(602,5,unit=UNIT)

    #asignar valores obtenidos
    porcentaje = 50
    presion = rh.registers[2]
    temperatura = int(rh.registers[3]/10)
    status_bomba = 1
    ciclos = rh.registers[4]
    cilindro = rh.registers[1]
    equipo = 1
    operador = 1002

    if temp_presion != presion or temp_temperatura != temperatura or temp_porcentaje != porcentaje or temp_ciclos != ciclos:
        #obtener fecha y hora actual
        now = datetime.now()
        dt_now = now.strftime("%Y-%m-%d %H:%M:%S")
        fecha = dt_now

        #cambiar valores temporales al detectar cambio
        temp_presion = presion
        temp_temperatura = temperatura
        temp_porcentaje = porcentaje
        temp_ciclos = ciclos

        #obtener cursor de conexión a base de datos
        cur = conn.cursor()

        try:
            #query para insertar en base de datos MariaDB
            cur.execute("INSERT INTO llenados "+
                        "(fecha,porcentaje,presion,temperatura,status_bomba,ciclos,cilindro,equipo,operador) "+
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (fecha, porcentaje, presion, temperatura, status_bomba, ciclos, cilindro, equipo, operador))

        except mariadb.Error as e:
            print(f"Error: {e}")
        
        #confirmar y deshacer transacciones
        conn.commit()
        #imprimir id del ultimo registro
        print(f"Último registro ID: {cur.lastrowid}")
        #cerrar conexion a base de datos
        conn.close
    else:
        print("Sin cambios")

    #cierre de conexión modbus
    client.close()

    #retraso de ejecución en segundos
    time.sleep(5)
    

    
