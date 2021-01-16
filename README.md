# Requisitos

* Sistema Operativo Linux
* Python 3 
* Apache Spark

# Instalación en Ubuntu

## Python
```bash
$ sudo apt-get install python3
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

En el caso de querer ejecutarlo en local, se debe ejecutar un comando como el siguiente:
```bash
$ spark-submit ____ arg1 arg2 
```

En el caso de querer ejecutarlo en un cluster, se debería ejecutar lo siguiente:
```bash
$ spark-submit --num-executors N --executor-cores M _____ arg1 arg2
```
	


