# Readme
This is a CLI tool (written in Python) that allows to load your data from Mysql to Google BigQuery. Allthough, I tried to optimize it for speed, there are some TODOs left. The script is based on [Ryan's import script](http://stackoverflow.com/a/28049671/609712).


## Requirements

sudo apt-get install libmysqlclient-dev

# install google cloud sdk
```
export CLOUD_SDK_REPO="cloud-sdk-$(lsb_release -c -s)"
echo "deb https://packages.cloud.google.com/apt $CLOUD_SDK_REPO main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key add -

sudo apt-get update && sudo apt-get install google-cloud-sdk
sudo apt-get install google-cloud-sdk-app-engine-python
```

## configure cloud sdk

```
gcloud init
```

create service account key and place it in this folder


## TODO
 - use async load?
 - adjust bucket-size
