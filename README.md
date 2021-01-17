# Requisitos

* Sistema Operativo Linux
* Python 2.7 
* Apache Spark

# Instalación en Ubuntu

## Python
```bash
$ sudo apt-get install python2.7
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

En el caso de querer ejecutar la aplicación con las funciones avanzadas:
```bash
$ spark-submit <nombre_script> <nombre_hotel> <distancia> 
```

Para ejecutar los scripts de *MapReduce* en modo local, se ejecuta el siguiente comando:
```bash
$ ./<nombre_mapper> <latitud> <longitud> <distancia> <min_avg*> < Hotel_Reviews_Large.csv | sort | ./<nombre_reducer>
```
Para ejecutarlos en un cluster, se recomienda utilizar la interfaz proporcionada por EMR, pasando como mapper el comando adecuado con sus argumentos, y especificando el path del mismo con la opcion *-files* en la sección de argumentos.

\***min_avg** sólo se necesita para el patrón 4.
