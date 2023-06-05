# Product Puller for Taxon Tek Stores
This repo holds the code for the Product Puller for Taxon Tek Stores.
The puller is meant to run periodically, getting any new products each run and pushing them into Taxon's Woo Store.

# Architecture
The puller is build with Python, and runs in the shared server, triggered by a cron expression every 10 minutes.

# How to Run Locally
There are two ways to run the puller locally:

### Directly run the `app.py` script.
First, install the local requirements.txt file

```commandline
pip install -r requirements.local.txt
```
The `app.py` acts as the entrypoint of the solution. By running it's main we can run the puller.
```commandline
python app.py
```


# Deploying
Deployed using ftp into the shared server.

```commandline
make deploy
```
