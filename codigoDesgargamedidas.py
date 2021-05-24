# -*- coding: utf-8 -*-
"""
Created on Thu May 20 19:28:12 2021

@author: Matias
"""

"""
Antes de empezar es necesario tener un usuario para la api de Twitter.
Abrir un proyecto en https://developer.twitter.com/en/apps
Generar las claves y guardarlas en un archivo .txt en la misma carpeta que codigo_General, con el siguiente formato:
    CONSUMER_KEY
    CONSUMER_SECRET
    ACCESS_TOKEN
    ACCES_TOKEN_SECRET
Importamos una librería que se llama codigo_General que tiene todas las funciones que vamos a usar

El código a continuación está armado como ejemplo, pero la idea es que puedan entenderlo para poder usarlo, modificarlo, copiar partes, etc.
"""


import importlib.util
import os

path_Matias = 'C:/Users/Matias/Documents/CursodatosTwitter/Curso_Twitter-master' # Este es el directorio donde está alojado el archivo codigo_General.py
path_Sofia = 'C:/Users/sofia/Curso/Curso_Twitter'
# Si estamos en la carpeta correcta podemos importar codigo_General import codigo_General

# Para evitar problemas de carpeta podemos en cambio hacer:
spec = importlib.util.spec_from_file_location('codigo_General', os.path.join(path_Sofia,'codigo_General.py'))
codigo_General = importlib.util.module_from_spec(spec)
spec.loader.exec_module(codigo_General)




# Definimos el conjunto de palabras para realizar la desarga

palabras = ['Alberto','Fase 1','restricciones', 'estricto', 'estricta', 'cuarentena', 'confinamiento'] # Defino conjunto de palabras
path_guardado_matias = 'C:/Users/Matias/Documents/CursodatosTwitter/proyectofinal' # Defino a dónde quiero que se guarden los archivos
path_guardado_sofia = 'C:/Users/sofia/Curso/Proy_medidas'
archivo_guardado = 'cuarentenaestricta.txt' # El nombre del archivo para que se guarden los tweets

# Inicializamos la descarga en vivo con el método Descargar_por_palabra_stream
codigo_General.Descargar_por_palabra_stream(Palabras = palabras, # Le pasamos las palabras para filtrar
                                            Archivo_Tweets = os.path.join(path_guardado_sofia,
                                                                          archivo_guardado), # Le pasamos la ubicación y nombre de archivo de destino
                                            idioma = ['es'], # indicamos el idioma
                                            ) 

#Otras opciones

codigo_General.Descargar_por_palabras(Palabras = palabras,
                                      Archivo_Tweets= os.path.join (path_guardado_sofia,
                                                                    archivo_guardado), 
                                      Cantidad=10000)

#codigo_General.Descargar_por_usuarios(usuario,path+Archivo_Tweets,Cantidad)