from datetime import datetime, timedelta
from airflow.models import Variable
import logging
from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.operators.python_operator import PythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.operators.bash import BashOperator
from airflow.models import Variable

from pyspark.sql import SparkSession
import requests
import pandas as pd
from pyspark.sql.functions import lit
from minio import Minio
from io import BytesIO

class CargaDadosDAG:
    def __init__(self):
        self.host = Variable.get("host")
        self.database = Variable.get("database")
        self.user = Variable.get("user")
        self.password = Variable.get("password")
        self.port = Variable.get("port")
        self.jdbc_url = f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

        self.minio_url = Variable.get("minio_url")
        self.minio_access = Variable.get("minio_access")
        self.minio_secret =Variable.get("minio_secret")
        self.arquivo = {}

        self.properties = {
            "user": self.user,
            "password": self.password,
            "driver": "org.postgresql.Driver"
        }

        self.spark = SparkSession.builder \
            .appName("Carga Dados") \
            .config("spark.executor.memory", "4g") \
            .config("spark.executor.instances", "5") \
            .config("spark.executor.cores", "2") \
            .config("spark.driver.memory", "4g") \
            .config('spark.sql.shuffle.partitions', '200') \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.2.18,org.apache.hadoop:hadoop-aws:3.3.1,com.google.guava:guava:30.1-jre,com.oracle.database.jdbc:ojdbc10-production:19.19.0.0") \
            .config('spark.hadoop.fs.s3a.access.key', self.minio_access) \
            .config('spark.hadoop.fs.s3a.secret.key', self.minio_secret) \
            .config('spark.hadoop.fs.s3a.endpoint', self.minio_url) \
            .config('spark.hadoop.fs.s3a.path.style.access', True) \
            .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
            .getOrCreate()

    def carregar_arquivo_parquet(self, ti):
        arquivo_pd = pd.read_parquet('https://storage.googleapis.com/challenge_junior/categoria.parquet')
        arquivo_spark = self.spark.createDataFrame(arquivo_pd)
    
        arquivo_dict = [row.asDict() for row in arquivo_spark.collect()]
        ti.xcom_push(key='arquivo_parquet', value=arquivo_dict) 


    def carregar_dados_jdbc(self, ti):
        query = f"""(SELECT * FROM public.venda) as vendas"""
        vendas_df = self.spark.read.jdbc(self.jdbc_url, query, properties=self.properties)
        
        vendas_dict = [row.asDict() for row in vendas_df.collect()]
        ti.xcom_push(key='vendas_df', value=vendas_dict) 

        vendas_df.show()


    def buscar_nome_vendedor(self, ti):
        vendas_dict = ti.xcom_pull(key='vendas_df', task_ids='carregar_dados_jdbc')
        vendas_df = self.spark.createDataFrame(vendas_dict)

        nomes = []
        for row in vendas_df.select("id_funcionario").distinct().collect():
            id_funcionario = row['id_funcionario']

            url = f"https://us-central1-bix-tecnologia-prd.cloudfunctions.net/api_challenge_junior?id={id_funcionario}"
            response = requests.get(url)

            nome_vendedor = response.text
            print(nome_vendedor)

            nomes.append(nome_vendedor)
        
        vendas_df = vendas_df.withColumn("nome_vendedor", lit(None)) 
        for idx, nome in enumerate(nomes):
            vendas_df = vendas_df.withColumn("nome_vendedor", lit(nome)) if idx == 0 else vendas_df.withColumn("nome_vendedor", vendas_df['nome_vendedor'])
        
        vendas_df.show()

        vendas_dict = [row.asDict() for row in vendas_df.collect()]
        ti.xcom_push(key='vendas_df', value=vendas_dict) 


    def realizar_join(self, ti):
        arquivo_dict = ti.xcom_pull(key='arquivo_parquet', task_ids='carregar_arquivo_parquet')
        arquivo_df = self.spark.createDataFrame(arquivo_dict)

        vendas_dict = ti.xcom_pull(key='vendas_df', task_ids='buscar_nome_vendedor')
        vendas_df = self.spark.createDataFrame(vendas_dict)
        
        vendas_df = vendas_df.join(arquivo_df, vendas_df.id_categoria == arquivo_df.id, 'left').select(
            vendas_df["*"], arquivo_df["nome_categoria"]
        )

        vendas_df.show()
        
        vendas_dict = [row.asDict() for row in vendas_df.collect()]
        ti.xcom_push(key='vendas_df', value=vendas_dict) 


    def salvar_dataframe(self, ti):
        arquivo_dict = ti.xcom_pull(key='vendas_df', task_ids='realizar_join')
        dataframe = self.spark.createDataFrame(arquivo_dict)

        dataframe.show()

        minio_client = Minio(
            self.minio_url,  
            access_key=self.minio_access,  
            secret_key=self.minio_secret, 
            secure=False 
        )

        bucket_name = "bix"

        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)

        dataframe.write \
            .mode("overwrite") \
            .parquet(f"s3a://bix/vendas/")


args = {
    "owner": "airflow",
    "retries": 0,
    "start_date": datetime(2023, 1, 1),
}

dag_name = "Vendas_Funcionario"
carga_dados_dag = CargaDadosDAG()

with DAG(
    dag_name,
    default_args=args,
    schedule_interval=None,
) as dag:
    
    carregar_arquivo_parquet = PythonOperator(
        task_id='carregar_arquivo_parquet',
        python_callable=carga_dados_dag.carregar_arquivo_parquet,
        retries=0,
    )

    carregar_dados_jdbc = PythonOperator(
        task_id='carregar_dados_jdbc',
        python_callable=carga_dados_dag.carregar_dados_jdbc,
        retries=0,
    )

    buscar_nome_vendedor = PythonOperator(
        task_id='buscar_nome_vendedor',
        python_callable=carga_dados_dag.buscar_nome_vendedor,
        retries=0,
    )

    realizar_join = PythonOperator(
        task_id='realizar_join',
        python_callable=carga_dados_dag.realizar_join,
        retries=0,
    )

    salvar_dataframe = PythonOperator(
        task_id='salvar_dataframe',
        python_callable=carga_dados_dag.salvar_dataframe,
        retries=0,
    )

carregar_arquivo_parquet >> carregar_dados_jdbc >> buscar_nome_vendedor >> realizar_join >> salvar_dataframe
