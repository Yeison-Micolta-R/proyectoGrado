from pathlib import Path
import socket
import numpy as np
import cv2
import tensorflow as tf

SERVER_IP = "127.0.0.1"  # Cambia esto a la IP de tu servidor
SERVER_PORT = 5000  # Puerto en el que el servidor está escuchando
MODEL_PATH = "C:/Users/USUARIO/Documents/ProyectoGrado/Versiones/Recursos/Modelo/conv.h5"  # Ruta al archivo del modelo entrenado

def cargar_modelo():
    # Cargar el modelo previamente entrenado
    modelo = tf.keras.models.load_model(MODEL_PATH)
    return modelo

def predecir_imagen(modelo, imagen):
    # Preprocesar la imagen según lo requerido por el modelo
    # (ajustar tamaño, normalizar, etc.)
    imagen_preprocesada = preprocess_image(imagen)
    
    # Realizar la predicción utilizando el modelo cargado
    predicciones = modelo.predict(np.array([imagen_preprocesada]))
    clase_predicha = np.argmax(predicciones)
    print(f"clase_predicha :{clase_predicha}")  
    # Devolver las predicciones (o el resultado según tu modelo)
    return predicciones, clase_predicha

def preprocess_image(imagen):
    # Realizar preprocesamiento de imagen según lo requiera tu modelo
    # Por ejemplo, cambiar tamaño, normalizar, etc.
    # Aquí se supone un preprocesamiento básico de cambiar tamaño a 28x28 y normalización
    imagen_p = cv2.resize(imagen, (28, 28))
    imagen_p = imagen_p.reshape((28, 28, 1))  # Añadir una dimensión para el batch y el canal
    #imagen_p = imagen_p.astype(np.float32) / 255.0  # Normalizar entre 0 y 1
    return imagen_p

def tamaño_bytes(bytes):
    if bytes <= 0xFF:
        byte_length = 1
    elif bytes <= 0xFFFF:
        byte_length = 2
    elif bytes <= 0xFFFFFFFF:
        byte_length = 4
    else:
        byte_length = 8
    return byte_length

def Bytesimages(image_path):
    lista_len_bytes = []
    for path in image_path:
        path = path.strip()
        ruta_archivo_str = str(path)  # Convertir el objeto Path a cadena
        print(f"File paths: {ruta_archivo_str}")
        try:
            with open(ruta_archivo_str, 'rb') as file:  # Abrir el archivo en modo binario
                ruta_archivo_bytes = file.read()  # Leer el contenido del archivo en bytes
            lista_len_bytes.append(ruta_archivo_bytes)  # Agregar al vector cada longitud de imagen
        except FileNotFoundError:
           print("error")
        
    return lista_len_bytes

def receive_data(sock):
    # Leer la longitud de los datos (4 bytes)
    length_bytes = sock.recv(4)
    length = int.from_bytes(length_bytes, byteorder='big')

    # Leer el número total de bytes
    data = b''
    while len(data) < length:
        packet = sock.recv(length - len(data))
        if not packet:
            return None
        data += packet

         # Decodificar los datos y reemplazar '\' con '/'
    decoded_data = data.decode('utf-8')#.replace('\\', '/')
    return decoded_data

def main():
    modelo = cargar_modelo()
   
    # Crea un socket TCP/IP
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        # Enlaza el socket a la dirección y el puerto
        server_socket.bind((SERVER_IP, SERVER_PORT))
        # Escucha las conexiones entrantes
        server_socket.listen()
        #print(f"Servidor escuchando en {SERVER_IP}:{SERVER_PORT}")     
        
        while True:
            print(f"Servidor escuchando en {SERVER_IP}:{SERVER_PORT}")  
            # Acepta una conexión entrante
            connection, client_address = server_socket.accept()
            try:
                print(f"Conexión establecida desde {client_address}")           
                #image_arrays = connection.recv(1024).decode('utf-8')
                image_arrays = receive_data(connection)
                print(f"tamaño de Bytesimages image_arrays  {len(image_arrays)}")  
                image_path = image_arrays.split(',')
                result = Bytesimages(image_path)
                print(f"tamaño de Bytesimages  {len(Bytesimages(image_path))}") 
                if result is None or not result:
                    # Envía un número específico si la lista está vacía o no se encontró algún archivo
                    connection.sendall(int(9999).to_bytes(4, byteorder='big'))
                    print("No se encontraron archivos, enviando 9999 al cliente.")
                    continue
                
                for img in Bytesimages(image_path):
                    print(f"total_data: {img[:20]}..")
                    print(f"Longitud de los bytes de la imagen recibidos: {len(img)}")

                    # Convierte los bytes en una matriz de imagen
                    nparr = np.frombuffer(img, np.uint8)
                    img_np = cv2.imdecode(nparr, cv2.IMREAD_GRAYSCALE)  # Asegúrate de leer la imagen como escala de grises

                    img_np = preprocess_image(img_np)

                    if img_np is None or img_np.size == 0:
                        print("Error: La imagen no se pudo decodificar o está vacía.")
                    else:
                        longitud_images = len(img)
                        byte_length = tamaño_bytes(longitud_images)

                        # Realizar predicción en la imagen
                        predicciones, clase = predecir_imagen(modelo, img_np)
                        
                        # Mostrar las predicciones
                        print("Predicciones: ", predicciones, " clase: ", clase)
                        print("byte_length: ", byte_length, " longitud_images: ", longitud_images)

                        # Convertir la clase predicha a un entero de Python nativo y enviarla
                        connection.sendall(int(clase).to_bytes(4, byteorder='big'))
                        print("Clase enviada:", clase)
                
            except Exception as e:
                print(f"Ocurrió un error: {e}")
            finally:
                connection.close()

if __name__ == "__main__":
    main()
