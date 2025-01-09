import json

# 1 - Crear registros clave-valor

def insertar_usuarios(conexion, usuarios):
    for usuario in usuarios:
        clave = f"usuario:{usuario['id_usuario']}"
        valor = json.dumps(usuario)
        conexion.set(clave, valor)
        print(f"Registro creado: {clave}")


# 2 - Obtener y mostrar el número de claves registradas

def contar_claves(conexion):
    claves = conexion.keys('*')
    total_claves = len(claves)
    print(f"Número total de claves registradas: {total_claves}")
    return total_claves

# 3 - Obtener y mostrar un registro en base a una clave

def obtener_registro_por_clave(conexion, clave):
    valor = conexion.get(clave)
    if valor:
        print(f"Registro de la clave '{clave}': {valor}")
    else:
        print(f"No se encontró ningún registro para la clave '{clave}'.")
    return valor


# 4 - Actualizar el valor de una clave y mostrar el nuevo valor(0.5 puntos)

def actualizar_registro(conexion, clave, nuevo_valor):
    print(f"Valor antes de actualizar: ") 
    obtener_registro_por_clave(conexion, clave)
    print("\n")
    conexion.set(clave, nuevo_valor)
    print(f"Valor después de actualizar la clave({clave}):{nuevo_valor}")


# 5 - Eliminar una clave-valor y mostrar la clave y el valor eliminado(0.5 puntos)

def eliminar_registro(conexion, clave):
    valor_eliminado = obtener_registro_por_clave(conexion, clave)
    
    conexion.delete(clave)
    
    print(f"Clave eliminada: {clave}")
    print(f"Valor eliminado: {valor_eliminado}")


# 6 - Obtener y mostrar todas las claves guardadas (0.5 puntos)

def obtener_todas_las_claves(conexion):
    claves = conexion.keys('*')
    print("Claves guardadas en nuesrtra Base de Datos de Redis:")
    for clave in claves:
        print(clave)

# 7 - Obtener y mostrar todos los valores guardados(0.5 puntos)

def obtener_todos_los_valores(conexion):
    claves = conexion.keys('*')
    print("Valores guardados en nuestra Base de Datos de Redis:")
    for clave in claves:
        valor = conexion.get(clave)
        print(f"Clave: {clave}, Valor: {valor}")

# 8 - Obtener y mostrar varios registros con una clave con un patrón en común usando * (0.5 puntos)

import json

def obtener_registros_con_patronasterisco(conexion, patron):
    claves = conexion.keys(patron)  # Busca todas las claves que coinciden con el patrón
    registros = []
    for clave in claves:
        valor = conexion.get(clave)  # Obtiene el valor de cada clave
        if valor:
            try:
                registro = json.loads(valor)  # Intenta convertir el valor en un diccionario Python
                # Filtra solo los registros que contienen el campo 'presentacion_video' y no es None
                if registro.get("presentacion_video"):
                    registros.append((clave, registro))
            except json.JSONDecodeError:
                print(f"Error al decodificar el valor de la clave {clave}. El valor no es un JSON válido.")
                continue  # Si el valor no es JSON, pasa al siguiente
    return registros


# 9 - Obtener y mostrar varios registros con una clave con un patrón en común usando [] (0.5 puntos)

def obtener_registros_con_patroncorchetes(conexion, patron):
    claves = conexion.keys(patron)
    registros = []
    for clave in claves:
        valor = conexion.get(clave)
        # Si es un JSON, lo parseamos
        try:
            registro = json.loads(valor)
        except json.JSONDecodeError:
            registro = valor
        registros.append((clave, registro))
    return registros


# 10 - Obtener y mostrar varios registros con una clave con un patrón en común usando ? (0.5 puntos)

# 11 - Obtener y mostrar varios registros y filtrarlos por un valor en concreto. (0.5 puntos)

# 12 - Actualizar una serie de registros en base a un filtro (por ejemplo aumentar su valor en 1 )(0.5 puntos)

# 13 - Eliminar una serie de registros en base a un filtro (0.5 puntos)

# 14 - Crear una estructura en JSON de array de los datos que vayais a almacenar(0.5 puntos)

# 15 - Realizar un filtro por cada atributo de la estructura JSON anterior (0.5 puntos)

# 16 - Crear una lista en Redis (0.5 puntos)

# 17 - Obtener elementos de una lista con un filtro en concreto(0.5 puntos)

# 18 - En Redis hay otras formas de almacenar datos: Set, Hashes, SortedSet,Streams, Geopatial, Bitmaps, Bitfields,Probabilistic y Time Series. Elige dos de estos tipos, y crea una función que los guarde en la base de datos y otra que los obtenga. (1.5 puntos)