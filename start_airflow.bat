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
