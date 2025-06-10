#!/bin/bash

# Script to run the complete pipeline in sequence:
# 1. make_inference_dataset.py - Prepares the dataset
# 2. inference.py - Runs the model inference
# 3. send_to_sheet.py - Sends results to Google Sheets
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
    poetry run python "$script_path" 2>&1 | tee -a $LOG_FILE
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
INFERENCE_SCRIPT="$PROJECT_ROOT/cedenar_anomalies/application/train.py"

# Check if scripts exist
for script in "$MAKE_SCRIPT" "$INFERENCE_SCRIPT"; do
    if [ ! -f "$script" ]; then
        echo "$(date) - ERROR - Script not found: $script" | tee -a $LOG_FILE
        exit 1
    fi
done

# Run scripts in sequence
if run_script "$MAKE_SCRIPT"; then
    if run_script "$INFERENCE_SCRIPT"; then
        echo "$(date) - INFO - Pipeline execution completed successfully" | tee -a $LOG_FILE
    else
        echo "$(date) - ERROR - Pipeline stopped due to failure in train.py" | tee -a $LOG_FILE
        exit 1
    fi
else
    echo "$(date) - ERROR - Pipeline stopped due to failure in make_train_dataset.py" | tee -a $LOG_FILE
    exit 1
fi

