# Ecommerce Data Pipeline com Apache Airflow

Protótipo de pipeline de dados de e-commerce desenvolvido com Apache Airflow, utilizando Docker Compose e Postgres para persistência de metadados.
O projeto demonstra a orquestração de tarefas de ETL/ELT para clientes, pedidos e produtos, com armazenamento intermediário em Amazon S3 e carga em Amazon Redshift para análise.
Os resultados são explorados em dashboards no Power BI, exibindo KPIs de negócio.
Inclui documentação completa, automação via script .bat e simulação de deploy em produção na AWS (ECR + ECS Fargate).
Em produção, o banco de metadados seria substituído por RDS Postgres, garantindo escalabilidade e persistência.

---

## 🎯 Objetivo
- Mostrar domínio de **Airflow** para orquestração de pipelines.  
- Demonstrar deploy local com **Docker Compose** (sem custo).  
- Simular deploy em produção na **AWS (ECR + ECS Fargate)**.  
- Automatizar inicialização com script `.bat` para praticidade.  

---

## 📌 Arquitetura

- **ETL/ELT**: DAGs do Airflow para carregar dados de clientes, pedidos e produtos. 
- **Warehouse**: PostgreSQL como metastore e destino dos dados (simulando RDS).
- **Dashboard**: Power BI conectado ao warehouse para visualização de KPIs.  
- **S3**: Armazenamento de arquivos intermediários.  
- **Redshift**: Data warehouse para análise.
- **Infraestrutura**: Docker + Docker Compose para orquestração.  

---

## 🚀 Como rodar localmente (Setup Manual) 

### 1. Clone o repositório
git clone https://github.com/seuusuario/ecommerce_pipeline.git 
cd ecommerce_pipeline

---

### 2. Pré-requisitos
- Docker e Docker Compose instalados.  
- Python 3.x para gerar Fernet Key.  

---

### 3. Gerar Fernet Key
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
Copie o valor e coloque no arquivo .env.

---

### 4. Arquivo .env
- Crie um arquivo .env na raiz do projeto com:

POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
POSTGRES_PORT=5433 # Porta externa mapeada para acesso fora do Docker

AIRFLOW__CORE__FERNET_KEY=<sua_chave_fernet>
AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
AIRFLOW__CORE__EXECUTOR=SequentialExecutor

> ℹ️ **Nota:** Dentro da rede Docker, o Airflow acessa o Postgres pela porta interna `5432`. 
> A variável `POSTGRES_PORT=5433` é usada apenas para acesso externo (ex.: pgAdmin ou cliente SQL local).

---

### 5. Subir containers
docker-compose up -d

---

### 6. Inicializar banco do Airflow
docker exec -it airflow-webserver airflow db init

---

### 7. Criar usuário admin
docker exec -it airflow-webserver airflow users create \
    --username airflow \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

---

### 8. Acessar UI
Abra http://localhost:8080  
Login: admin / admin


## 🚀 Como rodar localmente (⚡Inicialização automatizada com .bat)
- Este projeto inclui um script start_airflow.bat que automatiza todo o processo de inicialização e criação do usuário admin.

Conteúdo do arquivo:

      @echo off
      echo ============================================
      echo Inicializando ambiente Apache Airflow
      echo ============================================

      REM 1. Derruba containers antigos
      docker-compose down --volumes --remove-orphans

      REM 2. Sobe containers em segundo plano
      docker-compose up -d --build

      REM 3. Aguarda alguns segundos para o Postgres inicializar 
      echo Aguardando inicializacao do banco de dados...
      timeout /t 15 /nobreak >nul

      REM 4. Inicializa o banco de metadados usando o serviço airflow-webserver
      docker-compose run --rm airflow-webserver airflow db migrate

      REM 5. Cria usuário admin usando o serviço airflow-webserver
      docker-compose run --rm airflow-webserver airflow users create ^
      --username airflow ^
      --firstname Admin ^
      --lastname User ^
      --role Admin ^
      --email admin@example.com ^
      --password airflow

      REM 6. Reinicia containers para aplicar tudo
      docker-compose restart

      echo ============================================
      echo Ambiente pronto! Acesse http://localhost:8080
      echo Login: airflow / Senha: airflow
      echo ============================================
      pause

### 1. Como usar:
- Dê um duplo clique no arquivo start_airflow.bat.

- O script derruba containers antigos, sobe novos, inicializa o banco e cria o usuário admin automaticamente.

- Ao final, o ambiente estará pronto em http://localhost:8080.

- Login: airflow / airflow.

## ☁️ Deploy AWS (simulação)
Em produção, este projeto seria implantado na AWS:

### 1. Build da imagem Docker
docker build -t ecommerce-pipeline:latest .

### 2. Push para ECR
aws ecr get-login-password --region sa-east-1 | docker login --username AWS --password-stdin <account_id>.dkr.ecr.sa-east-1.amazonaws.com
docker tag ecommerce-pipeline:latest <account_id>.dkr.ecr.sa-east-1.amazonaws.com/ecommerce-pipeline:latest
docker push <account_id>.dkr.ecr.sa-east-1.amazonaws.com/ecommerce-pipeline:latest

### 3. ECS Fargate
- Criar cluster.
- Criar task definition apontando para a imagem no ECR.
- Configurar variáveis de ambiente (AIRFLOW_CONN_*, FERNET_KEY).
- Criar service para rodar webserver/scheduler.
- (Opcional) Load Balancer para expor a UI.

### 4. Banco de metadados
- Em produção seria RDS Postgres.
- No protótipo usamos Postgres local via Docker Compose para evitar custos.

## 📊 Demonstração

Abaixo algumas capturas de tela do projeto em execução:

### Airflow UI — DAGs em execução
![Airflow DAGs](screenshots/airflow_dags1.png)
![Airflow DAGs](screenshots/airflow_dags2.png)

### Power BI — Página 1: Visão Geral de Receita e Crescimento
Esta página mostra indicadores financeiros e evolução da receita entre setembro e dezembro de 2025.  
O foco está no ticket médio, receita total, crescimento mensal/trimestral e desempenho por categoria.

![Power BI Página 1](screenshots/powerbi_page1.png)

### Power BI — Página 2: Clientes e Pedidos
Esta página mostra retenção, aquisição e valor dos clientes entre setembro e dezembro de 2025.  
O foco está nos clientes de maior gasto e na evolução do churn.

![Power BI Página 2](screenshots/powerbi_page2.png)

### Power BI — Página 3: Produtos e Performance de Vendas
Esta página mostra a performance dos produtos e vendas entre setembro e dezembro de 2025.  
O foco está na classificação ABC, top produtos por receita, evolução temporal por categoria e itens vendidos.

![Power BI Página 3](screenshots/powerbi_page3.png)

### Execução do Script `.bat` — Inicialização automatizada
Abaixo, uma captura da execução do script `start_airflow.bat`, mostrando a criação dos containers, migração do banco de metadados e criação do usuário admin.

![Execução do .bat](screenshots/start_airflow_bat1.png)
![Execução do .bat](screenshots/start_airflow_bat2.png)

## 📝 Observação & 🔧 Notas Técnicas
- O atributo `version` foi removido do `docker-compose.yaml`, pois está obsoleto no Docker Compose v2.  
- A variável `AIRFLOW__CORE__SQL_ALCHEMY_CONN` foi atualizada para `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`, conforme recomendação das versões mais recentes do Airflow.  
- A porta `5433` definida em `POSTGRES_PORT` é a porta **externa** para acesso ao Postgres fora do Docker (ex.: pgAdmin).  
- Dentro da rede Docker, o Airflow continua acessando o Postgres pela porta interna `5432`.  
- O arquivo `.env.example` já reflete essas alterações, garantindo que o setup seja reproduzível sem warnings.
- A versão local usa Docker Compose com Postgres para persistência de metadados, sem custo.
- Também foi demonstrado como publicar a imagem no Amazon ECR e configurar execução no ECS Fargate, simulando o ambiente de produção.
- Em produção, o banco de metadados seria substituído por RDS Postgres e os serviços rodariam em ECS Fargate com escalabilidade automática.
- O script .bat facilita a inicialização automatizada, tornando o protótipo mais prático e profissional.
