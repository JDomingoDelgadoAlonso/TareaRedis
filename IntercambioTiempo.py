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
    # Mostrar el valor actual antes de actualizar
    print(f"Valor antes de actualizar: ") 
    obtener_registro_por_clave(conexion, clave)
    print("\n")
    
    # Convertir nuevo_valor en un diccionario si no lo es
    if isinstance(nuevo_valor, str):
        # Si es un string, lo convertimos a JSON (esto es opcional según el formato de entrada)
        nuevo_valor = json.loads(nuevo_valor)
    
    # Actualizar el registro en Redis
    conexion.set(clave, json.dumps(nuevo_valor))  # Guardar el nuevo valor como un JSON
    
    # Mostrar el valor actualizado
    print(f"Valor después de actualizar la clave ({clave}): {nuevo_valor}")


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
        # Obtener el tipo de dato de la clave
        tipo = conexion.type(clave)  # Ya devuelve un string
        
        if tipo == "string":
            valor = conexion.get(clave)  # Recuperar el valor del string
            print(f"Clave: {clave}, Valor: {valor}")  # No es necesario decodificar
        elif tipo == "list":
            valores = conexion.lrange(clave, 0, -1)  # Recuperar todos los valores de la lista
            # Decodificar los valores de la lista si es necesario
            valores = [v.decode('utf-8') if isinstance(v, bytes) else v for v in valores]
            print(f"Clave: {clave}, Lista: {valores}")
        else:
            print(f"Clave: {clave}, Tipo: {tipo} - No soportado")


# 8 - Obtener y mostrar varios registros con una clave con un patrón en común usando * (0.5 puntos)

import json

def obtener_registros_con_patronasterisco(conexion, patron):
    claves = conexion.keys(patron)  # Busca todas las claves que coinciden con el patrón
    registros = []
    
    for clave in claves:
        tipo = conexion.type(clave)  # Verifica el tipo de la clave
        
        if tipo == "string":
            valor = conexion.get(clave)  # Obtiene el valor de la clave de tipo string
            if valor:
                try:
                    registro = json.loads(valor)  # Intenta convertir el valor en un diccionario Python
                    # Filtra solo los registros que contienen el campo 'presentacion_video' y no es None
                    if registro.get("presentacion_video"):
                        registros.append((clave, registro))
                except json.JSONDecodeError:
                    print(f"Error al decodificar el valor de la clave {clave}. El valor no es un JSON válido.")
                    continue  # Si el valor no es JSON, pasa al siguiente
        
        elif tipo == "hash":
            # Si la clave es de tipo 'hash', obtenemos todos los campos
            valor = conexion.hgetall(clave)
            # Decodificar los valores de bytes a string solo si es necesario
            valor_decodificado = {k.decode('utf-8') if isinstance(k, bytes) else k: 
                                  v.decode('utf-8') if isinstance(v, bytes) else v 
                                  for k, v in valor.items()}
            if valor_decodificado.get("presentacion_video"):
                registros.append((clave, valor_decodificado))
        
        elif tipo == "zset":
            # Si la clave es de tipo 'zset', obtenemos los elementos con su puntaje
            elementos = conexion.zrange(clave, 0, -1, withscores=True)
            for elemento, puntaje in elementos:
                # Decodificar elemento si es necesario
                elemento_decodificado = elemento.decode('utf-8') if isinstance(elemento, bytes) else elemento
                # Lógica si se necesita filtrar el 'presentacion_video' para ZSET
                if "presentacion_video" in elemento_decodificado:
                    registros.append((clave, elemento_decodificado))
        
        # Si es de otro tipo que no nos interesa, no lo procesamos
        else:
            print(f"Clave {clave} tiene un tipo de dato no soportado para este filtrado.")
    
    return registros


# 9 - Obtener y mostrar varios registros con una clave con un patrón en común usando [] (0.5 puntos)

def obtener_registros_con_patroncorchetes(conexion, patron):
    claves = conexion.keys(patron)
    registros = []
    
    for clave in claves:
        valor = conexion.get(clave)
        try:
            # Intentamos convertir el valor en un diccionario Python
            registro = json.loads(valor)
        except json.JSONDecodeError:
            registro = valor  # Si no es JSON, mantenemos el valor tal cual
        registros.append((clave, registro))
    
    # Ahora mostramos directamente los registros
    if registros:
        print(f"Registros encontrados con el patrón '{patron}':")
        for clave, valor in registros:
            print(f"Clave: {clave} | Valor: {valor}")
    else:
        print(f"No se encontraron registros con el patrón '{patron}'")



# 10 - Obtener y mostrar varios registros con una clave con un patrón en común usando ? (0.5 puntos)

def obtener_registros_con_patron_con_interrogacion(conexion, patron):
    # Busca todas las claves que coinciden con el patrón
    claves = conexion.keys(patron)
    
    if not claves:
        print(f"No se encontraron claves con el patrón '{patron}'")
        return
    
    print(f"Registros encontrados con el patrón '{patron}':")
    
    for clave in claves:
        valor = conexion.get(clave)
        
        try:
            # Intentamos convertir el valor en un diccionario Python
            registro = json.loads(valor)
        except json.JSONDecodeError:
            # Si el valor no es un JSON válido, lo ignoramos
            continue
        
        # Imprimir la clave y su valor
        print(f"Clave: {clave} | Valor: {registro}")

# 11 - Obtener y mostrar varios registros y filtrarlos por un valor en concreto. (0.5 puntos)

def obtener_registros_filtrados(conexion, campo, valor):
    claves = conexion.keys('*')
    registros_filtrados = []

    # Buscar claves y filtrar por el campo y valor
    for clave in claves:
        tipo = conexion.type(clave)
        
        if tipo == "string":  # Solo procesamos claves tipo string
            valor_registro = conexion.get(clave)
            try:
                registro = json.loads(valor_registro)
            except json.JSONDecodeError:
                registro = valor_registro

            # Verificamos si el campo existe y su valor coincide
            if isinstance(registro, dict) and registro.get(campo) == valor:
                registros_filtrados.append((clave.decode('utf-8') if isinstance(clave, bytes) else clave, registro))
    
    # Mostrar los registros filtrados
    if registros_filtrados:
        print(f"Registros filtrados por el campo '{campo}' con el valor '{valor}':")
        for clave, valor in registros_filtrados:
            print(f"Clave: {clave} | Valor: {valor}")
    else:
        print(f"No se encontraron registros con el campo '{campo}' y el valor '{valor}'.")


def actualizar_registros_por_filtro(conexion, campo, valor_original, valor_nuevo):
    claves = conexion.keys('*')
    registros_actualizados = []

    for clave in claves:
        tipo = conexion.type(clave)  # Verificar el tipo de dato de la clave

        if tipo == "string":  # Solo procesamos claves tipo string
            valor_registro = conexion.get(clave)
            
            try:
                registro = json.loads(valor_registro)  # Intentamos parsear el valor en JSON
            except json.JSONDecodeError:
                registro = valor_registro  # Si no es JSON, tomamos el valor tal cual
            
            # Verificamos si el campo y valor coinciden con el filtro
            if isinstance(registro, dict) and registro.get(campo) == valor_original:
                registro[campo] = valor_nuevo  # Actualizamos el valor
                
                # Guardamos el registro actualizado de vuelta en Redis
                conexion.set(clave, json.dumps(registro))
                registros_actualizados.append((clave.decode('utf-8') if isinstance(clave, bytes) else clave, registro))
    
    # Mostrar los registros actualizados
    if registros_actualizados:
        print(f"Registros actualizados de '{campo}' de '{valor_original}' a '{valor_nuevo}':")
        for clave, valor in registros_actualizados:
            print(f"Clave: {clave} | Valor actualizado: {valor}")
    else:
        print(f"No se encontraron registros con el campo '{campo}' y el valor '{valor_original}'.")

# 13 - Eliminar una serie de registros en base a un filtro (0.5 puntos)

def eliminar_registros_sin_email(conexion):
    # Obtener todas las claves de usuarios
    claves = conexion.keys("usuario:*")
    registros_eliminados = 0

    # Recorrer todas las claves y verificar el campo 'email'
    for clave in claves:
        tipo = conexion.type(clave)

        if tipo == "string":  # Solo procesamos claves tipo string
            valor = conexion.get(clave)
            if valor:
                try:
                    # Convertir el valor de JSON a diccionario
                    registro = json.loads(valor)
                    # Verificar si el campo 'email' está vacío o es None
                    if not registro.get("email"):
                        # Si no tiene email, eliminar el registro
                        conexion.delete(clave)
                        registros_eliminados += 1
                        print(f"Registro eliminado: {clave} - {valor}")
                except json.JSONDecodeError:
                    print(f"Error al decodificar el valor de la clave {clave}. El valor no es un JSON válido.")
                    continue
        else:
            print(f"Clave {clave} no es de tipo string, tipo encontrado: {tipo}. No se procesó.")

    # Mostrar cuántos registros fueron eliminados
    print(f"\nTotal de registros eliminados: {registros_eliminados}")



# 14 - Crear una estructura en JSON de array de los datos que vayais a almacenar(0.5 puntos)

# Hecho en el apartado de insertar datos a redis, primer ejercicio.

# 15 - Realizar un filtro por cada atributo de la estructura JSON anterior (0.5 puntos)

def obtener_usuario_por_id(conexion, usuario_id):
    valor = conexion.get(usuario_id)
    if valor:
        print(f"Usuario con ID {usuario_id}: {valor}")
    else:
        print(f"No se encontró el usuario con ID {usuario_id}.")


def obtener_usuario_por_nombre(conexion, usuario_id):
    valor = conexion.get(usuario_id)
    if valor:
        usuario = json.loads(valor)
        print(f"Nombre del {usuario_id}: {usuario.get('nombre', 'No disponible')}")


def obtener_usuario_por_email(conexion, usuario_id):
    valor = conexion.get(usuario_id)
    if valor:
        usuario = json.loads(valor)
        print(f"Email del {usuario_id}: {usuario.get('email', 'No disponible')}")


def obtener_usuario_por_estado(conexion, usuario_id):
    valor = conexion.get(usuario_id)
    if valor:
        usuario = json.loads(valor)
        print(f"Estado del {usuario_id}: {usuario.get('estado', 'No disponible')}")


def obtener_video_usuario_por_id(conexion, usuario_id):
    valor = conexion.get(usuario_id)
    if valor:
        usuario = json.loads(valor)
        video = usuario.get("presentacion_video")
        if video:
            print(f"El  {usuario_id} tiene video. La dirección del video es: {video}")
        else:
            print(f"El  {usuario_id} no tiene video aún.")

def obtener_usuarios_por_rango_id(conexion):
    # Obtener los usuarios con IDs del 11 al 15
    usuarios_ids = [f"usuario:{id_usuario}" for id_usuario in range(11, 16)]
    
    # Llamar a las funciones para cada uno de estos usuarios
    for usuario_id in usuarios_ids:
        obtener_usuario_por_id(conexion, usuario_id)  # Buscar por ID
        obtener_video_usuario_por_id(conexion, usuario_id)  # Verificar video
        obtener_usuario_por_nombre(conexion, usuario_id)  # Buscar por nombre
        obtener_usuario_por_email(conexion, usuario_id)  # Buscar por email
        obtener_usuario_por_estado(conexion, usuario_id)  # Buscar por estado
        print("\n")
 

# 16 - Crear una lista en Redis (0.5 puntos)

def crear_lista_usuarios(conexion, lista_nombre, rango_inicio, rango_fin):
    """
    Crea una lista en Redis que contenga los ID y nombre de los usuarios dentro de un rango dado.
    """
    # Limpiar la lista si ya existe
    conexion.delete(lista_nombre)

    # Obtener los usuarios del rango dado
    for id_usuario in range(rango_inicio, rango_fin + 1):
        clave_usuario = f"usuario:{id_usuario}"
        valor = conexion.get(clave_usuario)
        
        if valor:
            # Convertimos el valor JSON a un diccionario
            try:
                registro = json.loads(valor)
                # Agregar un diccionario con id_usuario y nombre a la lista
                conexion.rpush(lista_nombre, json.dumps({
                    "id_usuario": registro["id_usuario"],
                    "nombre": registro["nombre"]
                }))
            except json.JSONDecodeError:
                print(f"Error al procesar el usuario con clave {clave_usuario}.")
    
    print(f"Lista '{lista_nombre}' creada con los usuarios del rango {rango_inicio}-{rango_fin}.")


def mostrar_lista(conexion, lista_nombre):
    """
    Muestra los elementos de una lista en Redis con ID y nombre de usuario.
    """
    elementos = conexion.lrange(lista_nombre, 0, -1)
    print(f"Elementos de la lista '{lista_nombre}':")
    for elemento in elementos:
        try:
            registro = json.loads(elemento)  # Convertir el valor de la lista en un diccionario
            print(f"- ID: {registro['id_usuario']}, Nombre: {registro['nombre']}")
        except json.JSONDecodeError:
            print(f"Error al procesar el elemento {elemento}.")

# 17 - Obtener elementos de una lista con un filtro en concreto(0.5 puntos)

def obtener_registros_filtrados_por_nombre(conexion, filtro_nombre):
    claves = conexion.keys('*')
    registros_filtrados = []

    # Buscar claves y filtrar por el nombre
    for clave in claves:
        tipo = conexion.type(clave)
        
        if tipo == "list":  # Si la clave es de tipo lista, queremos procesarla
            valores_lista = conexion.lrange(clave, 0, -1)  # Recuperar todos los elementos de la lista
            for item in valores_lista:
                # Convertir el valor a string para hacer la búsqueda
                try:
                    item_dict = json.loads(item)
                    # Verificamos si el 'nombre' contiene el filtro 'Pérez'
                    if isinstance(item_dict, dict) and 'nombre' in item_dict and filtro_nombre in item_dict['nombre']:
                        registros_filtrados.append((clave.decode('utf-8') if isinstance(clave, bytes) else clave, item_dict))
                except json.JSONDecodeError:
                    continue

    # Mostrar los registros filtrados
    if registros_filtrados:
        print(f"Registros filtrados por nombre con '{filtro_nombre}':")
        for clave, valor in registros_filtrados:
            print(f"Clave: {clave} | Valor: {valor}")
    else:
        print(f"No se encontraron registros con el nombre que contenga '{filtro_nombre}'.")


# 18
# Funciones para guardar datos en Redis

def guardar_hash(conexion, clave, datos):
    """
    Guarda un diccionario de datos en un Hash de Redis.
    """
    # Guardar los datos en el hash
    conexion.hset(clave, mapping=datos)
    print(f"Hash guardado en Redis bajo la clave: {clave}")


def guardar_sorted_set(conexion, clave, elementos):
    """
    Guarda un conjunto ordenado (ZSET) en Redis.
    Los elementos deben ser una lista de tuplas (elemento, puntaje).
    """
    for elemento, puntaje in elementos:
        conexion.zadd(clave, {elemento: puntaje})
    print(f"Sorted Set guardado en Redis bajo la clave: {clave}")


# Funciones para obtener datos de Redis

def obtener_hash(conexion, clave):
    """
    Obtiene los valores almacenados en un Hash de Redis.
    """
    datos = conexion.hgetall(clave)
    # Decodificar los valores de bytes a string solo si es necesario
    datos_decodificados = {k.decode('utf-8') if isinstance(k, bytes) else k: 
                           v.decode('utf-8') if isinstance(v, bytes) else v 
                           for k, v in datos.items()}
    print(f"Datos del Hash almacenados bajo la clave {clave}: {datos_decodificados}")


def obtener_sorted_set(conexion, clave):
    """
    Obtiene los elementos de un Sorted Set de Redis.
    """
    elementos = conexion.zrange(clave, 0, -1, withscores=True)
    # Mostrar los elementos junto con su puntaje
    print(f"Elementos del Sorted Set almacenado bajo la clave {clave}:")
    for elemento, puntaje in elementos:
        # Decodificar elemento si es necesario
        elemento_decodificado = elemento.decode('utf-8') if isinstance(elemento, bytes) else elemento
        print(f"{elemento_decodificado}: {puntaje}")


#
