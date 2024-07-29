# Set variables
PROJECT_ID="diploma-llm"
BUCKET_NAME="llm-converted-documents"
INSTANCE_NAME="document-converter"
REGION="europe-west4"
ZONE="europe-west4-a" # Change as per your preference
MACHINE_TYPE="e2-standard-4" # Change as per your requirements
IMAGE_FAMILY="debian-11" # Or another image family if preferred
IMAGE_PROJECT="debian-cloud"
STARTUP_SCRIPT="startup-script.sh" # Path to your startup script for app.jar
SERVICE_ACCOUNT_NAME="document-converter-account" # Replace with your service account name

# Set the default project ID
gcloud config set project $PROJECT_ID

gcloud services enable firestore.googleapis.com

# Create a new service account (if not already created)
gcloud iam service-accounts create $SERVICE_ACCOUNT_NAME --display-name "Document Converter Account"

# Grant the service account Firestore user role
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/datastore.user"

# Grant the service account storage object admin role
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/storage.objectAdmin"

# Create a Google Cloud Storage bucket
gsutil mb -l $REGION gs://$BUCKET_NAME/

# Set default ACLs or IAM policies as needed
# gsutil iam ch <members>:<role> gs://$BUCKET_NAME

# Create a Compute Engine instance
gcloud compute instances create $INSTANCE_NAME \
    --zone=$ZONE \
    --machine-type=$MACHINE_TYPE \
    --image-family=$IMAGE_FAMILY \
    --image-project=$IMAGE_PROJECT \
    --boot-disk-size=100GB \
    --boot-disk-type=pd-ssd \
    --scopes=https://www.googleapis.com/auth/cloud-platform \
    --metadata-from-file startup-script=$STARTUP_SCRIPT

# Output the external IP address
echo "Instance created. To access it, use the following command:"
echo "gcloud compute ssh $INSTANCE_NAME --zone $ZONE"
