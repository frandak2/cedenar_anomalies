#!/bin/bash

# Script to run the complete pipeline in sequence:
# 1. make_train_dataset.py - Prepares the dataset
# 2. train.py - Runs the model training
#
# This script respects the Poetry environment and project structure.

# Set up logging
LOG_FILE="pipeline_execution_train.log"
echo "$(date) - INFO - Starting the complete data processing pipeline" | tee -a $LOG_FILE

# Function to run a script and check for errors
run_script() {
    script_path=$1
    echo "$(date) - INFO - Starting execution of $script_path..." | tee -a $LOG_FILE
    
    start_time=$(date +%s)
    
    # Run the script through Poetry
    poetry run python "$script_path" "${@:2}" 2>&1 | tee -a $LOG_FILE
    exit_code=${PIPESTATUS[0]}
    
    end_time=$(date +%s)
    execution_time=$((end_time - start_time))
    
    if [ $exit_code -eq 0 ]; then
        echo "$(date) - INFO - Successfully executed $script_path in $execution_time seconds" | tee -a $LOG_FILE
        return 0
    else
        echo "$(date) - ERROR - Failed to execute $script_path. Exit code: $exit_code" | tee -a $LOG_FILE
        return 1
    fi
}

# Get the project root directory
PROJECT_ROOT="$(pwd)"

# Define script paths
MAKE_SCRIPT="$PROJECT_ROOT/cedenar_anomalies/application/make_train_dataset.py"
USER_SCRIPT="$PROJECT_ROOT/cedenar_anomalies/application/make_user_dataset.py"
TUNE_SCRIPT="$PROJECT_ROOT/cedenar_anomalies/application/tune_puntaje.py"
TRAIN_SCRIPT="$PROJECT_ROOT/cedenar_anomalies/application/train.py"

# Número de trials de Optuna (override con: TUNE_TRIALS=50 bash PipelineExecutionTrain.sh)
TUNE_TRIALS="${TUNE_TRIALS:-300}"

# Check if scripts exist
for script in "$MAKE_SCRIPT" "$USER_SCRIPT" "$TUNE_SCRIPT" "$TRAIN_SCRIPT"; do
    if [ ! -f "$script" ]; then
        echo "$(date) - ERROR - Script not found: $script" | tee -a $LOG_FILE
        exit 1
    fi
done

# Run scripts in sequence: make_train_dataset -> tune_puntaje (Optuna) -> train
if run_script "$MAKE_SCRIPT"; then
    if run_script "$USER_SCRIPT"; then
      if run_script "$TUNE_SCRIPT" --trials "$TUNE_TRIALS"; then
        if run_script "$TRAIN_SCRIPT"; then
            echo "$(date) - INFO - Pipeline execution completed successfully" | tee -a $LOG_FILE
        else
            echo "$(date) - ERROR - Pipeline stopped due to failure in train.py" | tee -a $LOG_FILE
            exit 1
        fi
      else
          echo "$(date) - ERROR - Pipeline stopped due to failure in tune_puntaje.py" | tee -a $LOG_FILE
          exit 1
      fi
    else
        echo "$(date) - ERROR - Pipeline stopped due to failure in make_user_dataset.py" | tee -a $LOG_FILE
        exit 1
    fi
else
    echo "$(date) - ERROR - Pipeline stopped due to failure in make_train_dataset.py" | tee -a $LOG_FILE
    exit 1
fi

