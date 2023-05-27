
# Para arrancar un fastapi:

""""
Dentro de la carpeta, localizada para Fast API crear un entorno virtual dentro de la misma (debe abrirce desde consloa de comandoos cmp * clic derecho abrir en terminal

alli ejecutar 
$ python3 -m venv fastapi-env 

creo el entorno luego lo arranco con:
$ fastapi-env\Scripts\activate.bat

me meto dentro del entorno #p windows
$ cd .\fastapi-env\


intalo libs:
$ pip install fastapi
$ pip install uvicorn

por si las dudas tmb:
$ pip install uvicorn[standard]

ahora creo dentro del entorno virtual el archivo main.py desde visual que contenga lo siguiente segun la instalacion de fastapi:


***
from typing import Union

from fastapi import FastAPI

app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/items/{item_id}")
def read_item(item_id: int, q: Union[str, None] = None):
    return {"item_id": item_id, "q": q}

***
y por ultimo ejecuto: 
$ python -m uvicorn main:app --reload


salida:

INFO:     Waiting for application startup.
INFO:     Application startup complete.


"""


from typing import Union
from fastapi import FastAPI
import pandas as pd
from typing import Dict

# Cargar el archivo pickle en un DataFrame
df_merged = pd.read_pickle('../../Datasets/Datasets_cleaned_ETL/Combinado_merged_movies_ratings/df_merged.pkl')



app = FastAPI()

#_________________________________________________________________________________________

# Docs: http://127.0.0.1:8000/docs#/

# Ruta: http://127.0.0.1:8000/
@app.get("/") # decorador cuando alguien cosulta la ruta"/" eecuta la funcion
def read_root():
    return {"Hello": "World"}


# Ruta: http://127.0.0.1:8000/items/5?q=somequery
@app.get("/items/{item_id}") # para consultas con variables ej librio n°1 ,2,3,4...
def read_item(item_id: int, q: Union[str, None] = None): # especifica el tipo de dato de entrada
    return {"item_id": item_id, "q": q}


# Ruta: http://127.0.0.1:8000/Libros/5?q=somequery
@app.get("/Libros/{item_id}") # para consultas con variables ej librio n°1 ,2,3,4...
def read_item(item_id: int):
    return {"libro n°:": item_id}

#____________________________________________________________________________________________________

# Funcion 1:
# Ruta: http://127.0.0.1:8000/get_max_duration/2021/{plataform}/min?platform=aws

@app.get("/get_max_duration/{year}/{plataform}/{duration_type}")
def get_max_duration(year: int, platform: str, duration_type: str):
    """get_max_duration of...### Funcion 1:

    #### Película (sólo película, no serie, etc)
    con mayor duración según año, plataforma y tipo de duración.
    La función debe llamarse get_max_duration(year, platform, duration_type) y 
    debe devolver sólo el string del nombre de la película.

    Args:
        year (int): 
        platform (str): 
        duration_type (str): 

    Returns:
        output_dict = {
        'plataforma': platform,
        'tipo_duracion': duration_type,
        'año': year,
        'titulo': max_duration_movie
    """
    # Filtrar el DataFrame para incluir solo las películas
    movies_df = df_merged[df_merged['type'] == 'movie']

    # Filtrar por año, plataforma y tipo de duración
    filtered_df = movies_df[(movies_df['release_year'] == year) & 
                            (movies_df['plataforma'] == platform) &
                            (movies_df['duration_type'] == duration_type)]
    
    # Verificar si el DataFrame filtrado está vacío
    if filtered_df.empty:
        return "No se encontraron películas que cumplan con los criterios especificados."
    
    # Obtener el título de la película con la duración máxima
    max_duration_movie = filtered_df.loc[filtered_df['duration_int'].idxmax(), 'title']
    
    # Crear el diccionario de salida
    output_dict = {
        'plataforma': platform,
        'tipo_duracion': duration_type,
        'año': year,
        'titulo': max_duration_movie
    }
    
    return output_dict


#____________________________________________________________________________________________________



# Funcion 2:
# Ruta: http://127.0.0.1:8000/get_score_count/netflix/3.5/2021

@app.get("/get_score_count/{plataforma}/{scored}/{anio}")
def get_score_count(plataforma: str, scored: float, anio: int) -> str:
    """get_score_count: ### Funcion 2:
    #### Cantidad de películas (sólo películas, no series, etc) según plataforma, 
    con un puntaje mayor a XX en determinado año. La función debe llamarse get_score_count(platform, scored, year) 
    y debe devolver un int, con el total de películas que cumplen lo solicitado.

    Args:
        plataforma (str): netflix, hulu, disney plus, aws
        scored (float): puntaje por encima de...
        anio (int): year/n

    Returns:
        output_dict = {
        'plataforma': plataforma,
        'cantidad': count,
        'anio': anio,
        'score': scored
    }
    """
    
    # Filtrar sólo las películas del año y plataforma solicitados
    movies2 = df_merged[(df_merged['plataforma'] == plataforma) & 
                       (df_merged['type'] == 'movie') &
                       (df_merged['release_year'] == anio)]

    # Contar las películas que cumplen con el puntaje mínimo
    count = (movies2['prom_rating'] >= scored).sum()
    
    
    return (f'Cantidad de peliculas: {count}')
#____________________________________________________________________________________________________



@app.get("/get_score_count2/{plataforma}/{scored}/{anio}")
