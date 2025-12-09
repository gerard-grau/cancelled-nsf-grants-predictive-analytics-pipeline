source init_airflow_gerard.sh

airflow users create \
    --username gerard_admin \
    --firstname Gerard \
    --lastname Grau \
    --role Admin \
    --email gerardgraugarcia@gmail.com

echo "Airflow Environment Loaded!"