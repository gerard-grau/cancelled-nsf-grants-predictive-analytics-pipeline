SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
export AIRFLOW_HOME="$SCRIPT_DIR/airflow"
echo "Airflow Environment Loaded!"
