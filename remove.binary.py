import string
import os
import re
import codecs
from pyspark import SparkContext, SparkConf

def main():

   conf = SparkConf().setMaster("local").setAppName("Deletebinaryfile")
   sc = SparkContext.getOrCreate(conf = conf)

   diretorio = ('/content/sample_data/spark')
   for diretorio, subpastas, arquivos in os.walk(diretorio):
     for arquivo in arquivos:
       files = sc.textFile(diretorio + "/"+ arquivo)
       arquivo = files.collect()
       saida = files.map(lambda x: ','.join([''.join(e for e in y if e in string.printable)
                    .strip('\"') for y in x
                    .split(',')]))
       
       print(arquivo)
       
       saida.coalesce(1).saveAsTextFile('/content/sample_data/spark/new.txt')
       
       print(saida.collect())


   sc.stop()

if __name__ == '__main__':
   main()
