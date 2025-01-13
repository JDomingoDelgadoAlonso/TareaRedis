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
        valor_registro = conexion.get(clave)
        
        try:
            registro = json.loads(valor_registro)
        except json.JSONDecodeError:
            registro = valor_registro
        
        # Verificamos si el campo existe y su valor coincide
        if isinstance(registro, dict) and registro.get(campo) == valor:
            registros_filtrados.append((clave, registro))
    
    # Mostrar los registros filtrados
    if registros_filtrados:
        print(f"Registros filtrados por el campo '{campo}' con el valor '{valor}':")
        for clave, valor in registros_filtrados:
            print(f"Clave: {clave} | Valor: {valor}")
    else:
        print(f"No se encontraron registros con el campo '{campo}' y el valor '{valor}'.")


# 12 - Actualizar una serie de registros en base a un filtro (por ejemplo aumentar su valor en 1 )(0.5 puntos)

def actualizar_registros_por_filtro(conexion, campo, valor_original, valor_nuevo):
    claves = conexion.keys('*')
    registros_actualizados = []

    for clave in claves:
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
            registros_actualizados.append((clave, registro))
    
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

# 17 - Obtener elementos de una lista con un filtro en concreto(0.5 puntos)

# 18 - En Redis hay otras formas de almacenar datos: Set, Hashes, SortedSet,Streams, Geopatial, Bitmaps, Bitfields,Probabilistic y Time Series. Elige dos de estos tipos, y crea una función que los guarde en la base de datos y otra que los obtenga. (1.5 puntos)