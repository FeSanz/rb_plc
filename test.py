import mariadb
import time
import sys
from datetime import datetime

UNIT = 0x1

#variables temporales para validacion de cambios
temp_presion = 0
temp_temperatura = 0
temp_porcentaje = 0
temp_ciclos = 0
temp_alarma = 0

# conexión a la base de datos MariaDB
try:
   conn = mariadb.connect(host="localhost", user="root", password="", database="infra")
except mariadb.Error as e:
   print(f"Error al conetarse a la base de datos: {e}")
   sys.exit(1)


while 1:
    #obtener valores de entrada
    inputPresion = input("Presión: ")
    inputTemperatura = input("Temperatura: ")
    inputPorcentaje = input("Porcentaje: ")
    inputCiclos = input("Ciclos: ")
    inputAlarma = input("Alarma: ")

    #asignar valores obtenidos
    porcentaje = inputPorcentaje
    presion = inputPresion
    temperatura = inputTemperatura
    status_bomba = 1
    ciclos = inputCiclos
    cilindro = 1
    equipo = 1
    operador = 1002
    #asignar valores obtenidos para alarma_tipos
    alarma = inputAlarma
    status = 1
    tipo = 1

    #obtener fecha y hora actual
    now = datetime.now()
    dt_now = now.strftime("%Y-%m-%d %H:%M:%S")
    fecha = dt_now


    if temp_presion != presion or temp_temperatura != temperatura or temp_porcentaje != porcentaje or temp_ciclos != ciclos:
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
        print(f"Último registro llenados ID: {cur.lastrowid}")
        #cerrar conexion a base de datos
        conn.close
    else:
        print("Sin cambios en lleandos")

    if temp_alarma != alarma:
        temp_alarma = alarma
        try:
            #obtener cursor de conexión a base de datos
            cur = conn.cursor()
            #query para insertar en base de datos MariaDB
            cur.execute("INSERT INTO alarmas "+
                        "(fecha, status, equipo, alarma_tipos) "+
                        "VALUES (?, ?, ?, ?)",
                        (fecha, status, equipo, tipo))

        except mariadb.Error as e:
            print(f"Error: {e}")

        #confirmar y deshacer transacciones
        conn.commit()
        #imprimir id del ultimo registro
        print(f"Último registro alarma ID: {cur.lastrowid}")
        #cerrar conexion a base de datos
        conn.close
    else:
        print("Sin cambios en alarmas")

    #retraso de ejecución en segundos
    time.sleep(5)
   

