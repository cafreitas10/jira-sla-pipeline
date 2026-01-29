"""
Bronze layer: Ingest raw data from Azure Blob Storage.

This module downloads JIRA issues data from Azure Blob Storage
using Service Principal authentication and saves it raw (unprocessed)
to the bronze layer.
"""

import os
import logging
from dotenv import load_dotenv
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


def authenticate_service_principal():
    """
    Authenticate to Azure using Service Principal credentials.

    Returns:
        ClientSecretCredential: Azure credential object or None if failed.
    """
    try:
        tenant_id = os.getenv('AZURE_TENANT_ID')
        client_id = os.getenv('AZURE_CLIENT_ID')
        client_secret = os.getenv('AZURE_CLIENT_SECRET')

        if not all([tenant_id, client_id, client_secret]):
            logger.error("Missing Service Principal credentials in environment variables")
            return None

        credential = ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret
        )

        logger.info("Service Principal authentication successful")
        return credential

    except Exception as error:
        logger.error(f"Service Principal authentication failed: {error}")
        return None


def read_blob_storage(account_url, container_name, blob_name, credential):
    """
    Read JSON file from Azure Blob Storage using Service Principal.

    Args:
        account_url (str): Azure Storage Account URL.
        container_name (str): Container name.
        blob_name (str): Blob file name.
        credential (ClientSecretCredential): Azure credential object.

    Returns:
        str: JSON content as string or None if failed.
    """
    try:
        blob_client = BlobClient(
            account_url=account_url,
            container_name=container_name,
            blob_name=blob_name,
            credential=credential
        )

        logger.info(f"Downloading blob: {blob_name}")

        # Download blob content
        blob_data = blob_client.download_blob()
        json_content = blob_data.readall().decode('utf-8')

        logger.info(f"Successfully downloaded: {len(json_content)} characters")
        return json_content

    except Exception as error:
        logger.error(f"Failed to download blob: {error}")
        return None


def save_bronze_layer(json_content, output_path):
    """
    Save raw JSON to bronze layer without any transformation.

    Args:
        json_content (str): Raw JSON content.
        output_path (str): Path to save JSON file.

    Returns:
        bool: True if successful, False otherwise.
    """
    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        with open(output_path, 'w', encoding='utf-8') as file:
            file.write(json_content)

        logger.info(f"Bronze layer saved (raw): {output_path}")
        return True

    except Exception as error:
        logger.error(f"Failed to save bronze layer: {error}")
        return False


def main():
    """
    Main ingest pipeline for bronze layer (raw data only).

    Returns:
        bool: True if successful, False otherwise.
    """
    # Load configuration
    account_url = os.getenv('ACCOUNT_URL')
    container_name = os.getenv('CONTAINER_NAME')
    blob_name = os.getenv('BLOB_NAME')

    # Validate configuration
    if not all([account_url, container_name, blob_name]):
        logger.error("Missing required environment variables: ACCOUNT_URL, CONTAINER_NAME, BLOB_NAME")
        return False

    # Authenticate with Service Principal
    credential = authenticate_service_principal()

    if credential is None:
        logger.error("Failed to authenticate with Service Principal")
        return False

    # Download from blob
    json_content = read_blob_storage(
        account_url,
        container_name,
        blob_name,
        credential
    )

    if json_content is None:
        logger.error("Failed to download JSON from blob")
        return False

    # Save raw JSON to bronze layer (NO TRANSFORMATION)
    bronze_output = "data/bronze/bronze_issues.json"
    success = save_bronze_layer(json_content, bronze_output)

    logger.info(f"Bronze layer ingest completed: {success}")
    return success


if __name__ == "__main__":
    main()
