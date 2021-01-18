# Requisitos

* Sistema Operativo Linux
* Python 2.7 
* Apache Spark
* Java
* Scala
* Numpy

# Instalación en una instancia EC2 de AWS - Ubuntu Server 18.04 LTS

## Java
```bash
$ sudo apt-get update
$ sudo apt install default-jre
```

## Scala
```bash
$ sudo apt-get install scala
```

## Python
```bash
$ sudo apt-get install python
```

## Numpy
```bash
$ sudo apt-get install python-pip
$ sudo pip install numpy
```

## Spark
```bash
$ sudo curl -O http://d3kbcqa49mib13.cloudfront.net/spark-2.2.0-bin-hadoop2.7.tgz
$ sudo tar xvf ./spark-2.2.0-bin-hadoop2.7.tgz
$ sudo mkdir /usr/local/spark
$ sudo cp -r spark-2.2.0-bin-hadoop2.7/* /usr/local/spark
```

## Configura el entorno 
Añade la siguiente línea al archivo .source:
```bash
$ export PATH="$PATH:/usr/local/spark/bin"
$ source ~/.profile
```

# Ejecución

En el caso de querer ejecutar los scripts de *Spark* en local, se debe ejecutar un comando como el siguiente:
```bash
$ spark-submit <nombre_script> <latitud> <longitud> <distancia> <min_avg*>
```
En el caso de querer ejecutarlos en un cluster, se debe ejecutar lo siguiente:
```bash
$ spark-submit --num-executors N --executor-cores M <nombre_script> <latitud> <longitud> <distancia> <min_avg*>
```

En el caso de querer ejecutar la aplicación con las funciones avanzadas local:
```bash
$ spark-submit analisis_hoteles_local.py <nombre_hotel> <distancia> 
$ Ejemplos de hoteles 'K K Hotel George' 'Hotel Arena'
```
En el caso de querer ejecutar la aplicación con las funciones avanzadas en cluster:
```bash
$ spark-submit analisis_hoteles_cluster.py <nombre_hotel> <distancia> 
$ Ejemplos de hoteles 'K K Hotel George' 'Hotel Arena'
```

Para ejecutar los scripts de *MapReduce* en modo local, se ejecuta el siguiente comando:
```bash
$ ./<nombre_mapper> <latitud> <longitud> <distancia> <min_avg*> < Hotel_Reviews_Large.csv | sort | ./<nombre_reducer>
```
Para ejecutarlos en un cluster, se recomienda utilizar la interfaz proporcionada por EMR, pasando como mapper el comando adecuado con sus argumentos, y especificando el path del mismo con la opcion *-files* en la sección de argumentos.

\***min_avg** sólo se necesita para el patrón 4.
