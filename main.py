from conexion import obtener_conexion_redis
from IntercambioTiempo import *

# Conexión a Redis
baseDatosRedis = obtener_conexion_redis()


usuarios = [
    {"id_usuario": "1", "nombre": "Juan Pérez", "email": "juanperez98765@gmail.com", "estado": "activo", "presentacion_video": None},
    {"id_usuario": "2", "nombre": "María López", "email": "marialopez9876543210@gmail.com", "estado": "inactivo", "presentacion_video": "https://video.maria.com"},
    {"id_usuario": "3", "nombre": "Carlos Gómez", "email": "carlosgomez12345@gmail.com", "estado": "activo", "presentacion_video": None},
    {"id_usuario": "4", "nombre": "Marta Sánchez", "email": "martasanchezlongemail1234567890@gmail.com", "estado": "activo", "presentacion_video": "https://video.marta.com"},
    {"id_usuario": "5", "nombre": "Luis Fernández", "email": "luisfernandezlong.email.hotmail@gmail.com", "estado": "inactivo", "presentacion_video": None},
    {"id_usuario": "6", "nombre": "Sara Martínez", "email": "sara.martinezextremelylongemail1234567890@gmail.com", "estado": "activo", "presentacion_video": "https://video.sara.com"},
    {"id_usuario": "7", "nombre": "Miguel Torres", "email": "migueltorres9876543210@gmail.com", "estado": "activo", "presentacion_video": None},
    {"id_usuario": "8", "nombre": "Elena Jiménez", "email": "elenajimenez6543210@gmail.com", "estado": "inactivo", "presentacion_video": "https://video.elena.com"},
    {"id_usuario": "9", "nombre": "Tomás Gómez", "email": "tomasgomezlongemail12345@gmail.com", "estado": "activo", "presentacion_video": None},
    {"id_usuario": "10", "nombre": "Lucía Fernández", "email": "luciafernandezlongemail1234@gmail.com", "estado": "activo", "presentacion_video": "https://video.lucia.com"}
]

print("====================================================================================================================================================")
print("1 - Crear registros clave-valor")
print("\n")
insertar_usuarios(baseDatosRedis, usuarios)
print("\n")
print("====================================================================================================================================================")
print("2 - Obtener y mostrar el número de claves registradas")
print("\n")
contar_claves(baseDatosRedis)
print("\n")
print("====================================================================================================================================================")
print("3 - Obtener y mostrar un registro en base a una clave")
print("\n")
obtener_registro_por_clave(baseDatosRedis, "usuario:1")
print("\n")
print("====================================================================================================================================================")
print("4 - Actualizar el valor de una clave y mostrar el nuevo valor")
print("\n")
actualizar_registro(baseDatosRedis, "usuario:1", "Juan Pérez")
print("\n")
print("====================================================================================================================================================")
print("5 - Eliminar una clave-valor y mostrar la clave y el valor eliminado")
print("\n")
eliminar_registro(baseDatosRedis, "usuario:2")
print("\n")
print("====================================================================================================================================================")
print("6 - Obtener y mostrar todas las claves guardadas")
print("\n")
obtener_todas_las_claves(baseDatosRedis)
print("\n")
print("====================================================================================================================================================")
print("7 - Obtener y mostrar todos los valores guardados")
print("\n")
obtener_todos_los_valores(baseDatosRedis)
print("\n")
print("====================================================================================================================================================")
print("8 - Obtener y mostrar varios registros con una clave con un patrón en común usando * ")
print("\n")
usuarios_con_video = obtener_registros_con_patronasterisco(baseDatosRedis, "usuario:*")
print("Registros de usuarios con presentación de video:")
for clave, valor in usuarios_con_video:
    print(f"Clave: {clave} | Valor: {valor}")
print("\n")
print("====================================================================================================================================================")
print("9 - Obtener y mostrar varios registros con una clave con un patrón en común usando [] ")
print("\n")
obtener_registros_con_patroncorchetes()
print("====================================================================================================================================================")
print("")
print("\n")
print("====================================================================================================================================================")
print("")
print("\n")
print("====================================================================================================================================================")
print("")
print("\n")
print("====================================================================================================================================================")
print("")
print("\n")
print("====================================================================================================================================================")
print("")