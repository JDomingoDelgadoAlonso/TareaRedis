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
nuevo_registro = {
    "id_usuario": "1",
    "nombre": "Juan Pérez",
    "email": "",
    "estado": "inactivo",
    "presentacion_video": None
}

print("4 - Actualizar el valor de una clave y mostrar el nuevo valor")
print("\n")
actualizar_registro(baseDatosRedis, "usuario:1", nuevo_registro)
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
obtener_registros_con_patroncorchetes(baseDatosRedis, "usuario:[1-6]")
print("\n")
print("====================================================================================================================================================")
print("10 - Obtener y mostrar varios registros con una clave con un patrón en común usando ?")
print("\n")
obtener_registros_con_patron_con_interrogacion(baseDatosRedis, "usuario:1?")
print("\n")
print("====================================================================================================================================================")
print("11 - Obtener y mostrar varios registros y filtrarlos por un valor en concreto.")
print("\n")
obtener_registros_filtrados(baseDatosRedis, "presentacion_video", None)
print("\n")
print("====================================================================================================================================================")
print("12 - Actualizar una serie de registros en base a un filtro (por ejemplo aumentar su valor en 1 )")
print("\n")
actualizar_registros_por_filtro(baseDatosRedis, 'estado', 'inactivo', 'activo')
print("\n")
print("====================================================================================================================================================")
print("13 - Eliminar una serie de registros en base a un filtro ")
print("\n")
eliminar_registros_sin_email(baseDatosRedis)
print("\n")
print("====================================================================================================================================================")
print("14 - Crear una estructura en JSON de array de los datos que vayais a almacenar")
usuarios2 = [
    {"id_usuario": "11", "nombre": "Gabri Pérez", "email": "gabriperez98765@gmail.com", "estado": "activo", "presentacion_video": None},
    {"id_usuario": "12", "nombre": "Godino Pérez", "email": "godinoerez98765@gmail.com", "estado": "activo", "presentacion_video": None},
    {"id_usuario": "13", "nombre": "Domi Pérez", "email": "domiperez98765@gmail.com", "estado": "inactivo", "presentacion_video": "https://video.domi.com"},
    {"id_usuario": "14", "nombre": "Javi Pérez", "email": "javiperez98765@gmail.com", "estado": "activo", "presentacion_video": None},
    {"id_usuario": "15", "nombre": "Lucía Pérez", "email": "luciaperez1234@gmail.com", "estado": "activo", "presentacion_video": "https://video.lucia.com"}
]
print("\n")
insertar_usuarios(baseDatosRedis, usuarios2)
print("\n")
print("====================================================================================================================================================")
print("15 - Realizar un filtro por cada atributo de la estructura JSON anterior")
print("\n")
obtener_usuarios_por_rango_id(baseDatosRedis)
print("\n")
print("====================================================================================================================================================")
print("16 - Crear una lista en Redis")
print("\n")
print("\n")
print("====================================================================================================================================================")
print("17 - Obtener elementos de una lista con un filtro en concreto")
print("\n")
print("\n")
print("====================================================================================================================================================")
print("18 - En Redis hay otras formas de almacenar datos: Set, Hashes, SortedSet,Streams, Geopatial, Bitmaps, Bitfields,Probabilistic y Time Series. Elige dos de estos tipos, y crea una función que los guarde en la base de datos y otra que los obtenga.")
print("18 - función que los guarde en la base de datos")
print("\n")
print("\n")
print("====================================================================================================================================================")
print("18 - función que los obtenga de la base de datos")
print("\n")
print("\n")
print("====================================================================================================================================================")
 