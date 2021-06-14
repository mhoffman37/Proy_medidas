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

path = 'C:/Users/Matias/Documents/CursodatosTwitter/Curso_Twitter-master' # Este es el directorio donde está alojado el archivo codigo_General.py

# Si estamos en la carpeta correcta podemos importar codigo_General import codigo_General

# Para evitar problemas de carpeta podemos en cambio hacer:
spec = importlib.util.spec_from_file_location('codigo_General', os.path.join(path,'codigo_General.py'))
codigo_General = importlib.util.module_from_spec(spec)
spec.loader.exec_module(codigo_General)
#%% Celda de procesamiento

# Procesemos los datos, para eso, debemos definir un par de archivos

path_guardado = 'C:/Users/Matias/Documents/CursodatosTwitter/Proyecto medidas - Archivero' # Defino a dónde quiero que se guarden los archivos
archivo_crudo = 'cuarentenaestricta.txt' # El nombre del archivo donde se guardaron los tweets
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
def filter_with_strings(raul, strings):
    raul = raul.loc[~(raul['tw_text'].str.contains(strings, case=False)) &
                    ~(raul['or_text'].str.contains(strings, case=False)), :]
    return raul


def filtrar_datos(datos):
        
    # 1) Filtramos los tweets y nos quedamos con aquellos que no tienen tw_location o que 
    # su tw_location incluye argentina
    datos['tw_location'] = datos['tw_location'].fillna('').apply(lambda x: x.strip())
    datos = datos.loc[(datos['tw_location'].str.contains('Argentina')) | 
                      (datos['tw_location'] == '') | 
                      (pd.isna(datos['tw_location'])), :]
    
    datos['tw_text'] = datos['tw_text'].fillna('')
    datos['or_text'] = datos['or_text'].fillna('')
    # 2) Podriamos filtrar por la aparición de Yuri/Yuti en tw_text y en or_text
    datos = filter_with_strings(datos, 'yuri|yuti')

    # 3) ídem pero con Fujimori|FUJIMORI|fujimori
    datos = filter_with_strings(datos, 'fujimori')

    # 4) Bárcena
    datos = filter_with_strings(datos, 'bárcena|barcena')

    # 5) Carlos Alberto Maya
    datos = filter_with_strings(datos, 'maya')

    # 6) Alberto Chang
    datos = filter_with_strings(datos, 'chang')

    # 7) @OmarPrietoGob 
    datos = filter_with_strings(datos, '@OmarPrietoGob')
    
    # 8) Mujica
    datos = filter_with_strings(datos, 'mujica')

    # 9) Alberto Carasquilla
    datos = filter_with_strings(datos, 'carasquilla')

    # 10) Alberto Tejada 
    datos = filter_with_strings(datos, 'tejada')
    
    # 11) Nicolas Maduro 
    datos = filter_with_strings(datos, 'nicolas maduro|nicolás maduro')

    # 12) Alberto Valero
    datos = filter_with_strings(datos, 'valero')

    # 13) Guastatoya
    datos = filter_with_strings(datos, 'guastatoya')

    # 14) @pelaez_alberto
    datos = filter_with_strings(datos, '@pelaez_alberto')

    # 15) barroco
    datos = filter_with_strings(datos, 'barroco')

    # 16) Limache
    datos = filter_with_strings(datos, 'limache')

    return datos
    
    
base_de_datos.tweets = filtrar_datos(base_de_datos.tweets)
base_de_datos.tweets.shape


# print(base_de_datos.usuarios)
print(f'Hay {len(base_de_datos.tweets)} tweets')
print(base_de_datos.tweets['tw_location'].value_counts())
#%% Celda de aplicación de algunos métodos

# Analicemos cómo se distribuyen los tipos de tweets
base_de_datos.plot_tipo_tweet()
# Analicemso cómo se distribuyen los usuarios según su rol
base_de_datos.plot_rol_usuario()
# Nube de palabras
base_de_datos.plot_nube() #fecha_inicial='2021-05-12', fecha_final='2021-05-13')
# Principales Hashtags
base_de_datos.plot_principales_Hashtags() #fecha_inicial='2021-05-12', fecha_final='2021-05-13')
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
#%%
#Acá estoy creando los nombres de los archivos para armar los grafos

archivo_grafo_usuarioRT= 'Grafo_Medidas_UsuarioRT.gexf'
archivo_grafo_menciones= 'Grafo_Medidas_Menciones.gexf'
archivo_grafo_usuarioQT= 'Grafo_Medidas_UsuarioQT.gexf'

archivo_grafo_hashtags= 'Grafo_Medidas_Hashtags.gexf'

#Y acá estoy llenando esos archivos con los grafos. Los vamos a abrir con gephi
base_de_datos.armar_grafo(tipo='usuarios',archivo_grafo=path_guardado+archivo_grafo_usuarioRT, tipo_enlace='RT', dirigido=False)
base_de_datos.armar_grafo(tipo='usuarios', archivo_grafo=path_guardado+archivo_grafo_usuarioQT, tipo_enlace='QT', dirigido=False)

base_de_datos.armar_grafo(tipo='hashtags', archivo_grafo=path_guardado+archivo_grafo_hashtags)

base_de_datos.armar_grafo(tipo='menciones', archivo_grafo=path_guardado+archivo_grafo_menciones)
#%%

#Acá miré un poco qué hacía todo esto, peeero...
base_de_datos.grafo
base_de_datos.Grafo_Medidas_Hashtags
print(nx.info(base_de_datos.grafo))
base_de_datos.grafo.nodes['fabipa90']

#Acá generé una división en comunidades Louvain para los archivos tipo usuario y guardé las modificaciones.

base_de_datos.agregar_comunidades_Louvain(grafo='usuarios')
base_de_datos.guardar_grafo(path_guardado+archivo_grafo_usuarioRT)
base_de_datos.guardar_grafo(path_guardado+archivo_grafo_usuarioQT)