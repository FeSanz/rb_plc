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

#variables temporales para validacion de cambios
'''[0]presion, [1]temperatura, [2]porcentaje, [3]ciclos, [4]alarma'''
temp_rh = [0, 0, 0, 0, 0]
temp_ri = [0, 0, 0, 0, 0]
temp_rj = [0, 0, 0, 0, 0]
temp_rk = [0, 0, 0, 0, 0]
temp_rl = [0, 0, 0, 0, 0]

#conexión a modbus con ip-puerto
client = ModbusClient('192.168.1.200', port=502)

# conexión a la base de datos MariaDB
try:
   conn = mariadb.connect(host="localhost", user="spaceuser", password="condor", database="infra")
except mariadb.Error as e:
   print(f"Error al conetarse a la base de datos: {e}")
   sys.exit(1)

# registro de llenados a base de datos
def RegistrarLlenado(fecha, porcentaje, presion, temperatura, status_bomba, ciclos, cilindro, equipo, operador):
    cur = conn.cursor()

    try:
        cur.execute("INSERT INTO llenados "+
                    "(fecha, porcentaje, presion, temperatura, status_bomba, ciclos, cilindro, equipo, operador) "+
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (fecha, porcentaje, presion, temperatura, status_bomba, ciclos, cilindro, equipo, operador))

    except mariadb.Error as e:
        print(f"Error: {e}")

    conn.commit()
    print(f"Último registro de llenado equipo " + str(equipo) +" ID: {cur.lastrowid}")
    conn.close

# registro de alarmas a base de datos
def RegistrarAlarma(fecha, status, equipo, alarma_tipos):
    cur = conn.cursor()
    try:
        cur.execute("INSERT INTO alarmas "+
                    "(fecha, status, equipo, alarma_tipos) "+
                    "VALUES (?, ?, ?, ?)",
                    (fecha, status, equipo, alarma_tipos))

    except mariadb.Error as e:
        print(f"Error: {e}")

    conn.commit()
    print(f"Último registro de alarma equipo" + str(equipo) +" ID: {cur.lastrowid}")
    conn.close


while 1:
    #obtener fecha y hora actual
    now = datetime.now()
    dt_now = now.strftime("%Y-%m-%d %H:%M:%S")
    fecha = dt_now

    #conexion modbus
    client.connect()

    #obtener arreglo de datos de cada equipo
    rh = client.read_holding_registers(602,8,unit=UNIT)
    ri = client.read_holding_registers(612,8,unit=UNIT)
    rj = client.read_holding_registers(622,8,unit=UNIT)
    rk = client.read_holding_registers(632,8,unit=UNIT)
    rl = client.read_holding_registers(642,8,unit=UNIT)

    '''[0]Alarma [1]Cilindro [2]Presion [3]Temperatura [4]Ciclos [5]Porcentaje [6]Status_Bomba [7]Operador'''

    #Valida cambio (presion || temperatura || porcentaje || ciclos) 1er equipo
    if temp_rh[0] != rh.registers[2] or temp_rh[1] != int(rh.registers[3]/10) or temp_rh[2] != rh.registers[5] or temp_rh[3] != rh.registers[4]:
        temp_rh[0] = rh.registers[2]
        temp_rh[1] = int(rh.registers[3]/10)
        temp_rh[2] = rh.registers[5]
        temp_rh[3] = rh.registers[4]

        RegistrarLlenado(fecha, rh.registers[5], rh.registers[2], int(rh.registers[3]/10), 1, rh.registers[4], rh.registers[1], 1, 1002)
    else:
        print("Sin cambios en llenados equipo 1")

    if rh.registers[0] != 0 and temp_rh[4] != rh.registers[0]: 
        temp_rh[4] = rh.registers[0]
        RegistrarAlarma(fecha, 1, 1, rh.registers[0])
    else:
        temp_rh[4] = rh.registers[0]
        print("Sin cambios en alarmas equipo 1")

    #Valida cambio (presion || temperatura || porcentaje || ciclos) 2er equipo
    if temp_ri[0] != ri.registers[2] or temp_ri[1] != int(ri.registers[3]/10) or temp_ri[2] != ri.registers[5] or temp_ri[3] != ri.registers[4]:
        temp_ri[0] = ri.registers[2]
        temp_ri[1] = int(ri.registers[3]/10)
        temp_ri[2] = ri.registers[5]
        temp_ri[3] = ri.registers[4]

        RegistrarLlenado(fecha, ri.registers[5], ri.registers[2], int(ri.registers[3]/10), 1, ri.registers[4], ri.registers[1], 2, 1002)
    else:
        print("Sin cambios en llenados equipo 2")

    if ri.registers[0] != 0 and temp_ri[4] != ri.registers[0]: 
        temp_ri[4] = ri.registers[0]
        RegistrarAlarma(fecha, 1, 2, ri.registers[0])
    else:
        temp_ri[4] = ri.registers[0]
        print("Sin cambios en alarmas equipo 2")

    #Valida cambio (presion || temperatura || porcentaje || ciclos) 3er equipo
    if temp_rj[0] != rj.registers[2] or temp_rj[1] != int(rj.registers[3]/10) or temp_rj[2] != rj.registers[5] or temp_rj[3] != rj.registers[4]:
        temp_rj[0] = rj.registers[2]
        temp_rj[1] = int(rj.registers[3]/10)
        temp_rj[2] = rj.registers[5]
        temp_rj[3] = rj.registers[4]

        RegistrarLlenado(fecha, rj.registers[5], rj.registers[2], int(rj.registers[3]/10), 1, rj.registers[4], rj.registers[1], 3, 1002)
    else:
        print("Sin cambios en llenados equipo 3")

    if rj.registers[0] != 0 and temp_rj[4] != rj.registers[0]: 
        temp_rj[4] = rj.registers[0] 
        RegistrarAlarma(fecha, 1, 3, rj.registers[0])
    else:
        temp_rj[4] = rj.registers[0] 
        print("Sin cambios en alarmas equipo 3")


    #Valida cambio (presion || temperatura || porcentaje || ciclos) 4to equipo
    if temp_rk[0] != rk.registers[2] or temp_rk[1] != int(rk.registers[3]/10) or temp_rk[2] != rk.registers[5] or temp_rk[3] != rk.registers[4]:
        temp_rk[0] = rk.registers[2]
        temp_rk[1] = int(rk.registers[3]/10)
        temp_rk[2] = rk.registers[5]
        temp_rk[3] = rk.registers[4]

        RegistrarLlenado(fecha, rk.registers[5], rk.registers[2], int(rk.registers[3]/10), 1, rk.registers[4], rk.registers[1], 4, 1002)
    else:
        print("Sin cambios en llenados equipo 4")

    if rk.registers[0] != 0 and temp_rk[4] != rk.registers[0]: 
        temp_rk[4] = rk.registers[0] 
        RegistrarAlarma(fecha, 1, 4, rk.registers[0])
    else:
        temp_rk[4] = rk.registers[0]
        print("Sin cambios en alarmas equipo 4")

    #Valida cambio (presion || temperatura || porcentaje || ciclos) 5to equipo
    if temp_rl[0] != rl.registers[2] or temp_rl[1] != int(rl.registers[3]/10) or temp_rl[2] != rl.registers[5] or temp_rl[3] != rl.registers[4]:
        temp_rl[0] = rl.registers[2]
        temp_rl[1] = int(rl.registers[3]/10)
        temp_rl[2] = rl.registers[5]
        temp_rl[3] = rl.registers[4]

        RegistrarLlenado(fecha, rl.registers[5], rl.registers[2], int(rl.registers[3]/10), 1, rl.registers[4], rl.registers[1], 5, 1002)
    else:
        print("Sin cambios en llenados equipo 5")

    if rl.registers[0] != 0 and temp_rl[4] != rl.registers[0]: 
        temp_rl[4] = rl.registers[0] 
        RegistrarAlarma(fecha, 1, 5, rl.registers[0])
    else:
        temp_rl[4] = rl.registers[0]
        print("Sin cambios en alarmas equipo 5")


    #cierre de conexión modbus
    client.close()
    #retraso de ejecución en segundos
    time.sleep(5)
    

    
