#!/bin/bash

# Set the paths
PROJECT_DIR="/Users/soumilshah/IdeaProjects/icebergpython/Arrow/Lab8"
SCRIPT_PATH="$PROJECT_DIR/server/duckdb_server.py"
VENV_PATH="/Users/soumilshah/IdeaProjects/icebergpython/Arrow/lab7/myenv"  # Updated to correct venv path

# Ensure the virtual environment exists
if [ ! -d "$VENV_PATH" ]; then
    echo "Error: Virtual environment not found at $VENV_PATH"
    exit 1
fi

# Number of instances to launch (default to 3 if not provided as an argument)
N=${1:-3}

# Starting port number
BASE_PORT=10000

# Function to launch a server instance
launch_server() {
    local port=$1
    local shard=$2
    osascript -e "tell app \"Terminal\"
        do script \"cd $PROJECT_DIR/db && source $VENV_PATH/bin/activate && python3 $SCRIPT_PATH --port $port --shard $shard\"
    end tell"
}

# Launch N instances with consecutive ports
for ((i=0; i<N; i++)); do
    PORT=$((BASE_PORT + i))
    SHARD="shard$((i+1))"
    launch_server $PORT $SHARD
    sleep 1  # Short delay to prevent rapid spawning issues
done

echo "Launched $N server instances starting from port $BASE_PORT."
