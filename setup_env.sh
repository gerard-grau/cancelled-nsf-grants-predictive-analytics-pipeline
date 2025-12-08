SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
export AIRFLOW_HOME="$SCRIPT_DIR/airflow"
source "$SCRIPT_DIR/../../venv-BDA/bin/activate"
echo "Airflow Environment Loaded!"
