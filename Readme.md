# Readme
This is a CLI tool (written in Python) that allows you to load your data from Mysql to Google BigQuery. Although, I tried to optimize it for speed, there are some TODOs left. The script is based on [Ryan's import script](http://stackoverflow.com/a/28049671/609712).

## Requirements

 * libmysqlclient-dev (on ubuntu just run: `sudo apt install libmysqlclient-dev`) 
 * Google Cloud SDK
 * Python

### Install Google Cloud SDK
The following steps are based on the [official Google Dokumentation](https://cloud.google.com/sdk/docs/#deb):

```
export CLOUD_SDK_REPO="cloud-sdk-$(lsb_release -c -s)"
echo "deb https://packages.cloud.google.com/apt $CLOUD_SDK_REPO main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key add -

sudo apt-get update && sudo apt-get install google-cloud-sdk
sudo apt-get install google-cloud-sdk-app-engine-python
```

To configure it, run:
```
gcloud init
```

To authenticate you app you can create a service account key, name it `google_key.json` and place it in this folder.

## Install dependencies
I would recommend to use a virtualenv. Run the following command to install all dependencies using pip:

```
pip install -r requirements.txt
```


## Usage
```
Usage: run.py [OPTIONS]

Options:
  -h, --host TEXT           MySQL hostname
  -d, --database TEXT       MySQL database  [required]
  -u, --user TEXT           MySQL user
  -p, --password TEXT       MySQL password
  -t, --table TEXT          MySQL table  [required]
  -i, --projectid TEXT      Google BigQuery Project ID  [required]
  -n, --dataset TEXT        Google BigQuery Dataset name  [required]
  -l, --limit INTEGER       max num of rows to load
  -s, --batch_size INTEGER  max num of rows to load
  --help                    Show this message and exit.
```


## TODO
 - use async load?
 - adjust bucket-size
