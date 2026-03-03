# Base oficial do Airflow
FROM apache/airflow:2.7.0

# Definir diretório de trabalho
WORKDIR /opt/airflow

# Copiar dependências e instalar
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar DAGs e scripts para dentro do container
COPY dags/ /opt/airflow/dags/
COPY scripts/ /opt/airflow/scripts/

# Entrypoint padrão do Airflow
ENTRYPOINT ["/entrypoint"]
CMD ["webserver"]
