# proyecto_electorales

Guia de instalacion y ejecucion del proyecto de analisis de twe

## Instalacion de MongoDB

Para la creacion de la base de datos no realcional MongoDB se debe descargar el programa MongoDB: https://www.mongodb.com/es/products/compass

## Despliegue del ambiente de trabajo.

Se debe entrar en la carpeta conedora del archivo manage.py y correr el siguiente comando para crear la base de datos en SQLite
```sh
$ python3 manage.py migrate
```

Para poder correr los scripts y algoritmos desde la consola.
```sh
$ python3 manage.py shell
```
Para importar las funciones
```sh
>>> from datos.views import *
```
