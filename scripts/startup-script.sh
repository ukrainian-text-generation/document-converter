#!/bin/bash

# Update the instance
sudo apt-get update
sudo apt-get upgrade -y

# Install Java (OpenJDK 11 in this example)
sudo apt-get install -y openjdk-17-jre
sudo apt-get install -y poppler-utils

# Create a directory for the app
APP_DIR="/opt/myapp"
sudo mkdir -p $APP_DIR
sudo chmod 777 $APP_DIR

SOURCE_BUCKET_NAME="llm-raw-documents"
TARGET_BUCKET_NAME="llm-converted-documents"
APP_BUCKET_NAME="processor-bin"
PROJECT_ID="diploma-llm"
DATABASE_ID="(default)"
COLLECTION_ID="raw-documents"
RETRIES=3
BATCH_SIZE=100


# Copy the app.jar from GCS bucket
gsutil cp gs://$APP_BUCKET_NAME/converter-app.jar $APP_DIR/

# Navigate to the app directory
cd $APP_DIR

# Run the application
# (Assuming the application does not need additional parameters or environment variables)
java -jar converter-app.jar $SOURCE_BUCKET_NAME $TARGET_BUCKET_NAME $PROJECT_ID $DATABASE_ID $COLLECTION_ID $RETRIES $BATCH_SIZE

# Shutdown the instance after execution is complete
sudo shutdown -h now