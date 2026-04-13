from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import os

# Definição dos caminhos (dentro do container)
DATA_PATH = "/opt/airflow/data"
ENTRADA = f"{DATA_PATH}/entrada.csv"
TASK2 = f"{DATA_PATH}/task2.csv"
TASK3 = f"{DATA_PATH}/task3.csv"
TASK4 = f"{DATA_PATH}/task4.csv"
MEDIA_FILE = f"{DATA_PATH}/media_avaliacao.csv"
TOTAL_ARTISTA_FILE = f"{DATA_PATH}/total_artista.csv"

default_args = {
    'owner': 'analytics',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 13),
    'retries': 1,
}

dag = DAG(
    'streaming_pipeline',
    default_args=default_args,
    description='Pipeline de dados de streaming musical',
    schedule_interval=None,
    catchup=False,
    tags=['streaming', 'pandas'],
)

# Task 1: Copiar dados-stream.csv para entrada.csv
t1 = BashOperator(
    task_id='copiar_arquivo',
    bash_command=f'cp {DATA_PATH}/dados-stream.csv {ENTRADA}',
    dag=dag,
)

# Task 2: Tratar datas (formato dd/mm/aaaa)
def tratar_datas():
    df = pd.read_csv(ENTRADA)
    # Converte coluna de data (nome da coluna? Vamos supor 'data_stream')
    # O dataset exemplo tem coluna 'data_avaliacao'? Ajuste conforme necessário
    # Como não temos o CSV exato, vou generalizar: tenta converter qualquer coluna com 'data' ou 'date'
    for col in df.columns:
        if 'data' in col.lower() or 'date' in col.lower():
            # Tenta converter primeiro do formato ISO (yyyy-mm-dd)
            df[col] = pd.to_datetime(df[col], errors='coerce')
            # Depois formata para dd/mm/aaaa
            df[col] = df[col].dt.strftime('%d/%m/%Y')
    df.to_csv(TASK2, index=False)

t2 = PythonOperator(
    task_id='tratar_datas',
    python_callable=tratar_datas,
    dag=dag,
)

# Task 3: Remover linhas com nome_musica vazio e retornar quantidade descartada
def remover_vazios(**context):
    df = pd.read_csv(TASK2)
    antes = len(df)
    df_limpo = df[df['nome_musica'].notna() & (df['nome_musica'].str.strip() != '')]
    depois = len(df_limpo)
    descartados = antes - depois
    df_limpo.to_csv(TASK3, index=False)
    # Envia o total descartado via XCom
    context['ti'].xcom_push(key='total_descartados', value=descartados)

t3 = PythonOperator(
    task_id='remover_linhas_vazias',
    python_callable=remover_vazios,
    provide_context=True,
    dag=dag,
)

# Task 4: Inserir na tabela descartados (usando PostgresHook diretamente ou operador)
def inserir_descartados(**context):
    total = context['ti'].xcom_pull(task_ids='remover_linhas_vazias', key='total_descartados')
    hook = PostgresHook(postgres_conn_id='airflow_db')  # nome da connection configurada no Airflow
    hook.run("INSERT INTO descartados (total) VALUES (%s)", parameters=(total,))

t4 = PythonOperator(
    task_id='inserir_descartados',
    python_callable=inserir_descartados,
    provide_context=True,
    dag=dag,
)


# Task 5: Consultar tabela genero_musical e retornar resultado como dicionário
def consultar_generos(**context):
    hook = PostgresHook(postgres_conn_id='airflow_db')
    records = hook.get_records("SELECT id_genero, nome_genero FROM genero_musical")
    # Transforma em dicionário {'001':'POP', ...}
    genero_dict = {row[0]: row[1] for row in records}
    context['ti'].xcom_push(key='genero_map', value=genero_dict)

t5 = PythonOperator(
    task_id='consultar_generos',
    python_callable=consultar_generos,
    provide_context=True,
    dag=dag,
)

# Task 6: Enriquecer task3.csv com nome_genero
def enriquecer_csv(**context):
    genero_map = context['ti'].xcom_pull(task_ids='consultar_generos', key='genero_map')
    df = pd.read_csv(TASK3)
    df['nome_genero'] = df['id_genero'].map(genero_map)
    df.to_csv(TASK4, index=False)

t6 = PythonOperator(
    task_id='enriquecer',
    python_callable=enriquecer_csv,
    provide_context=True,
    dag=dag,
)

# Task 7: Média de avaliação por música
def media_avaliacao():
    df = pd.read_csv(TASK4)
    # Supondo colunas: nome_musica, avaliacao (nota)
    media = df.groupby('nome_musica')['avaliacao'].mean().reset_index()
    media.columns = ['musica', 'media_avaliacao']
    media.to_csv(MEDIA_FILE, index=False)

t7 = PythonOperator(
    task_id='media_por_musica',
    python_callable=media_avaliacao,
    dag=dag,
)

# Task 8: Total de músicas ouvidas por artista
def total_artista():
    df = pd.read_csv(TASK4)
    # Supondo colunas: artista, total_audicoes ou apenas contagem de linhas
    # Como o enunciado diz "total de músicas ouvidas por artista", pode ser contagem de streams
    # Se houver coluna 'quantidade_streams' some, senão conta ocorrências
    if 'quantidade_streams' in df.columns:
        total = df.groupby('artista')['quantidade_streams'].sum().reset_index()
    else:
        total = df.groupby('artista').size().reset_index(name='total_musicas_ouvidas')
    total.to_csv(TOTAL_ARTISTA_FILE, index=False)

t8 = PythonOperator(
    task_id='total_por_artista',
    python_callable=total_artista,
    dag=dag,
)

# Task 9: Remover entrada.csv (sempre executar, mesmo se tasks anteriores falharem)
t9 = BashOperator(
    task_id='remover_entrada',
    bash_command=f'rm -f {ENTRADA}',
    trigger_rule='all_done',   # executa independente de sucesso/falha das upstreams
    dag=dag,
)

# Task 10: Fim do pipeline
t10 = DummyOperator(
    task_id='fim',
    dag=dag,
)

# Definição das dependências
t1 >> t2 >> t3 >> t4 >> t5 >> t6
t6 >> [t7, t8]
[t7, t8] >> t9 >> t10