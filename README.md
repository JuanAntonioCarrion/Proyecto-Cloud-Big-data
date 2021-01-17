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

En el caso de querer ejecutar los scripts de Spark en local, se debe ejecutar un comando como el siguiente:
```bash
$ spark-submit <nombre_script> <latitud> <longitud> <distancia> <min_avg*>
```
En el caso de querer ejecutarlos en un cluster, se debe ejecutar lo siguiente:
```bash
$ spark-submit --num-executors N --executor-cores M <nombre_script> <latitud> <longitud> <distancia> <min_avg*>
```
* Sólo para pattern_4.py

En el caso de querer ejecutar la aplicación avanzada:
```bash
$ spark-submit <nombre_script> <nombre_hotel> <distancia> 
```
	


