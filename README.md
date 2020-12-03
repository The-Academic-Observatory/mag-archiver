# MAG Archiver
MAG Archiver is an Azure Function App that automatically archives Microsoft Academic Graph (MAG) releases so that they 
can be transferred to other cloud services.

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python Version](https://img.shields.io/badge/python-3.7-blue)](https://img.shields.io/badge/python-3.7-blue)
![Python package](https://github.com/The-Academic-Observatory/mag-archiver/workflows/Python%20package/badge.svg)
[![codecov](https://codecov.io/gh/The-Academic-Observatory/mag-archiver/branch/develop/graph/badge.svg)](https://codecov.io/gh/The-Academic-Observatory/mag-archiver)

## Status
This is a proof of concept; the functionality for archiving and compressing each MAG release has not been implemented yet.

## Setup
The following instructions explain how to setup Mag Archiver.

### Dependencies
* [Install Azure CLI](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli?view=azure-cli-latest)
* [Install Azure Functions Core Tools](https://docs.microsoft.com/en-us/azure/azure-functions/functions-run-local?tabs=linux%2Ccsharp%2Cbash)
* [Create an Azure Storage Account](https://docs.microsoft.com/en-us/azure/storage/common/storage-account-create?tabs=azure-portal)
  * Region: choose an Azure region that is close to the other cloud provider that you want to transfer the data to.
  * Access tier: hot (need to be able to delete containers without cold storage deletion fees)
  * Create container: mag-snapshots
  * Under Blob Service > Lifecycle Management > Code view: add the life-cycle rules from lifecycle-rules.json
    * These rules move blobs to the cold tier after 30 days and delete the blobs after 61 days.
* [Create an Azure Function App](https://docs.microsoft.com/en-us/azure/azure-functions/functions-create-function-app-portal)
  * Take note of your function app name, you will need it later.
  * Under Settings > Configuration > Application settings, add the following Application settings (name: value):
    * STORAGE_ACCOUNT_NAME: the name of your storage account.
    * STORAGE_ACCOUNT_KEY: they key for your storage account.
    * TARGET_CONTAINER: mag-snapshots
* [Subscribe to Microsoft Academic Graph on Azure storage](https://docs.microsoft.com/en-us/academic-services/graph/get-started-setup-provisioning) 

### Deploy to Azure
To deploy mag-archiver follow the instructions below.

#### Setup Azure account
Make sure that the Azure account that your Function App is deployed to is set as the default.

To do this, list your accounts and copy the id of the account that should be the default account:
```bash
az account list
```

Set the account to the Azure account that your Function App is deployed to:
```bash
az account set -s <insert your account id here>
```

Check that the correct account is set, you should see your account show up:
```bash
az account show
```

#### Deploy the Function App
Clone the project:
```bash
git clone git@github.com:The-Academic-Observatory/mag-archiver.git
```

Enter the function app folder:
```bash
cd mag-archiver
```

Deploy the function:
```bash
func azure functionapp publish <your function app name> --python
```

## Architecture
The architecture of MAG Archiver is illustrated via the deployment and process view diagrams below.

### Process View
The MAG subscription adds each new MAG release as a new Azure Blob storage container in the user's Azure Storage 
account.

An [Azure Function App](https://azure.microsoft.com/en-us/services/functions/) runs every 10 minutes and checks to 
see if any new MAG release containers have been added.

![process view](https://raw.githubusercontent.com/The-Academic-Observatory/mag-archiver/develop/docs/process_view.svg)

Metadata about which MAG releases have been discovered and processed are stored in an 
[Azure Table Storage](https://azure.microsoft.com/en-us/services/storage/tables/) table called `MagReleases`. 
The `MagReleases` table is also used used to enable the Apache Airflow MAG workflow to query and find out what MAG 
releases have finished processing and where on the Azure blob storage container they can be downloaded from. A 
[share access signature (SAS)](https://docs.microsoft.com/en-us/azure/storage/common/storage-sas-overview) 
with read only privileges is used to provide the Apache Airflow MAG workflow with access to the table.

When the Function App finds a new MAG release, it copies the files from the container onto a shared container called
`mag-snapshots` under a folder with the same name as the container it was copied from. After copying the files, the 
original container is deleted. 

The Function App copies the MAG files to a shared container so that the Apache Airflow MAG workflow only needs to 
hold a single SAS token, one for the shared container. In the future the copying of files by the Cloud Function can be 
replaced by a service that compresses the files, as shown in the diagram above.

A total of two SAS tokens are shared: one for the `MagReleases` table and one for the `mag-snapshots` container.

### Deployment View
The deployment view below shows what services are used and where they are deployed. 

![deployment view](https://raw.githubusercontent.com/The-Academic-Observatory/mag-archiver/develop/docs/deployment_view.svg)

