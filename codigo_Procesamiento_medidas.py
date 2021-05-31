#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 12 09:05:56 2021

@author: tcicchini

En este código, vamos a realizar las siguientes tareas:
    - Procesar un código previamente descargado. Es decir, queremos obtener el archivo de procesado
    tipo tabla y el archivo que contiene información de los usuarios
    - Inicializar el obejto Base_Datos
    - Cargar en dicho objetos los archivos previamente procesados
    - Aplicar algunos métodos definidos previamente
    
    - Además, vamos a jugar un poco con la librería pandas


"""

import importlib.util # Esta librería la usamos para importar codigo_General
import os # Esta librería la usamos para acoplarno sen cierta forma a la estructura de nuestro sistema operativo

path = 'C:/Users/sofia/Curso/Curso_Twitter/' # Este es el directorio donde está alojado el archivo codigo_General.py

# Si estamos en la carpeta correcta podemos importar codigo_General import codigo_General

# Para evitar problemas de carpeta podemos en cambio hacer:
spec = importlib.util.spec_from_file_location('codigo_General', os.path.join(path,'codigo_General.py'))
codigo_General = importlib.util.module_from_spec(spec)
spec.loader.exec_module(codigo_General)
#%% Celda de procesamiento

# Procesemos los datos, para eso, debemos definir un par de archivos

path_guardado = 'C:/Users/sofia/Curso/Proy_medidas/' # Defino a dónde quiero que se guarden los archivos
archivo_crudo = 'cuarentenaestricta_2021_05_21.txt' # El nombre del archivo donde se guardaron los tweets
archivo_procesado_tweets = 'ProcesadoTweets_cuarentenaestricta_2021_05_21.tx'
archivo_procesado_usuarios = 'ProcesadoUsuarios_cuarentenaestricta_2021_05_21.tx'

codigo_General.procesamiento(archivo_tweets = os.path.join(path_guardado, # Le pasamos el archivo de los datos en crudo
                                                           archivo_crudo
                                                           ),
                             archivo_guardado = os.path.join(path_guardado, # Le pasamos el archivo a donde queremos que nos guarde el procesamiento de tweets
                                                             archivo_procesado_tweets
                                                             ),
                             archivo_usuarios = os.path.join(path_guardado, # Le pasamos el archivo a donde queremos que nos guarde el procesamiento de usuarios
                                                             archivo_procesado_usuarios
                                                             ),
                             )

#%% Celda de levantar la base de datos
base_de_datos = codigo_General.Bases_Datos() # Inicializamos el objeto, no hace falta pasarle argumentos


# Así como está, la base de datos está'vacía. Necesitamos cargarle los archivos procesados

# Traemos entonces los tweets
base_de_datos.cargar_datos(archivo_datos = os.path.join(path_guardado, # Le pasamos el archivo donde están los tweets procesados
                                                        archivo_procesado_tweets
                                                        )
                           )
# print(base_de_datos.tweets)
# Traemos entonces los usuarios
base_de_datos.cargar_usuarios(archivo_usuarios = os.path.join(path_guardado, # Le pasamos el archivo donde está la info de usuarios procesada
                                                              archivo_procesado_usuarios
                                                              )
                              )

import pandas as pd
def filtrar_datos(datos):
    # 1) Filtramos los tweets y nos quedamos con aquellos que no tienen tw_location o que 
    # su tw_location incluye argentina
    datos['tw_location'] = datos['tw_location'].fillna('').apply(lambda x: x.strip())
    datos = datos.loc[(datos['tw_location'].str.contains('Argentina')) | 
                      (datos['tw_location'] == '') | 
                      (pd.isna(datos['tw_location'])), :]
    
    datos['tw_text'] = datos['tw_text'].fillna('')
    datos['or_text'] = datos['or_text'].fillna('')
    # 2) Podriamos filtrar por la aparición de Yuri en tw_text
    datos = datos.loc[~(datos['tw_text'].str.contains('Yuri|YURI|yuri')) &
                      ~(datos['or_text'].str.contains('Yuri|YURI|yuri')), :]  
    
    # 3) ídem pero con Fujimori|FUJIMORI|fujimori
    datos = datos.loc[~(datos['tw_text'].str.contains('Fujimori|FUJIMORI|fujimori')) &
                      ~(datos['or_text'].str.contains('Fujimori|FUJIMORI|fujimori')), :]  

    return datos
    
    
base_de_datos.tweets = filtrar_datos(base_de_datos.tweets)

import json
import simplejson
sample = base_de_datos.tweets.sample(10)
for index, row in sample.iterrows():
    d = dict(row)
    del d['tw_created_at']
    del d['or_created_at']
    print(simplejson.dumps(d, ignore_nan=True))
    # print(json.dumps(d))
    # print(dict(row))


# print(base_de_datos.usuarios)
print(f'Hay {len(base_de_datos.tweets)} tweets')
print(base_de_datos.tweets['tw_location'].value_counts())
#%% Celda de aplicación de algunos métodos

# Analicemos cómo se distribuyen los tipos de tweets
base_de_datos.plot_tipo_tweet()
# Analicemso cómo se distribuyen los usuarios según su rol
base_de_datos.plot_rol_usuario()
# Nube de palabras
base_de_datos.plot_nube(fecha_inicial='2021-05-12', fecha_final='2021-05-13')
# Principales Hashtags
base_de_datos.plot_principales_Hashtags(fecha_inicial='2021-05-12', fecha_final='2021-05-13')
# Principales Usuarios
base_de_datos.plot_principales_Usuarios(metrica_interes = 'or_favCount',fecha_inicial='2021-05-12', fecha_final='2021-05-13')
# Análisis temporal
base_de_datos.plot_evolucion_temporal(fecha_inicial='2021-05-12 7:00:00', fecha_final='2021-05-13', frecuencia='5min')

#%% Celda para indagar en pandas
import pandas as pd # Traigo pandas por las dudas

df_tweets = base_de_datos.tweets.copy() # Hacemos esta copia, para ahorrarnos notación
print(type(df_tweets)) # Vemos que es un DataFrame de pandas

# Podemos pedirle información sobre la tabla en general
print(df_tweets.info())

# Podemos obtener las columnas con el siguiente método
print(df_tweets.columns)

# Podemos ver los valores y la cantidad de veces que aparece cada uno para una dada columna
print(df_tweets['relacion_nuevo_original'].value_counts())

# Dada una columna numérica, podemos aplicar algunas métricas de estadística
print(df_tweets['or_user_followers_count'].mean())

# Podemos agrupar datos y aplicar transformaciones a los datos agregados
print(df_tweets.groupby(pd.Grouper(key = 'or_created_at',
                                   freq = '1min')
                        )['relacion_nuevo_original'].count()
      )




