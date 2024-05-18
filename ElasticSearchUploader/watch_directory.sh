#!/bin/bash

# Directory to watch (set this to the directory you want to watch)
WATCH_DIR="$HOME/parquet-files"

# Path to your JAR file
JAR_PATH="$HOME/parquet-files/ElasticSearchUploader.jar"

# Function to handle new files
handle_new_file() {
    local file_path="$1"
    echo "New file detected: $file_path"
    
    # Run your JAR file with the new file as an argument
     java --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
         --add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
         -jar "$JAR_PATH" "$file_path"
}

# Export the function to make it available to the sub-shell
export -f handle_new_file
export JAR_PATH

# Start watching the directory for new files
inotifywait -m -r -e create --format '%w%f' "$WATCH_DIR" | while read NEW_FILE
do
    # Check if the new file ends with .parquet
    if [[ "$NEW_FILE" == *.parquet ]]; then
        # Call the handler function with the new file path
        handle_new_file "$NEW_FILE"
    fi
done

