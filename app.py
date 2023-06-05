import math
import sys
from datetime import datetime
import traceback
import numpy as np
import pandas as pd
import pytz

sys.path.append("/home/aukiozgq/order_puller/")
from src.connectors.config_connector import ConfigConnector
from src.connectors.file_connector import FileConnector
from src.connectors.woo_connector import WooConnector
from src.logging.logger import get_module_logger
from src.mappers import map_products, FINAL_COLUMNS
from src.settings import Config

logger = get_module_logger(__name__)
BATCH_SIZE = 100


def main():
    """
    Main handler for the Puller.
    Gets all new products and pushes them into Woo
    :return:
    """

    try:
        config_connector = ConfigConnector()
        config = config_connector.get_config()
        dryrun = config.get("dryrun", True)
        cleanup = config.get("cleanup", True)
        str_last_check = config.get("last_check", "")
        logger.info(msg="")
        if dryrun:
            logger.info(msg="Starting Puller in dryrun.")
        else:
            logger.info(msg="Starting Puller.")
        logger.info(msg="")
        if str_last_check:
            last_check = str_last_check
        else:
            last_check = (
                datetime.now(pytz.timezone("UTC"))
                .replace(hour=0, minute=0, second=0, microsecond=0)
                .astimezone(pytz.utc)
                .strftime(Config.DATE_FORMAT)
            )

        now = (
            datetime.now(pytz.timezone("UTC"))
            .astimezone(pytz.utc)
            .strftime(Config.DATE_FORMAT)
        )
        logger.info(msg=f"Last check: {last_check}")
        logger.info(msg=f"Current check: {now}")

        sku_filter = load_sku_filter(config=config)

        download_all_sources(config=config)
        all_products_df = load_and_transform(config=config, sku_filter=sku_filter)
        if not dryrun:
            send_products_to_woo(all_new_products_df=all_products_df)

            logger.info(msg="Writing updated config.")
            config["last_check"] = now
            config_connector.set_config(config)
    except Exception as error:
        tb = traceback.format_exc()
        error_message = (
            f"Unexpected error while processing data. \n"
            f"Error: {error}. \n"
            f"Traceback: {tb}"
        )
        logger.error(msg=error_message)
    finally:
        if cleanup:
            delete_all_local_sources(config=config)
    logger.info(msg="")
    logger.info(msg="Puller finished")
    logger.info(msg="")


def load_sku_filter(config):
    doc_url = config.get("skus_doc_url")
    sheet_name = config.get("skus_doc_sheet_name").replace(" ", "%20")
    csv_url = f"{doc_url}/export?gid=0&format=csv&sheet={sheet_name}"
    df = pd.read_csv(csv_url, header=None, on_bad_lines="skip")
    df.columns = ["sku"]
    return df.sku.to_list()


def download_all_sources(config):
    file_connector = FileConnector()
    sources = config.get("sources", [])
    for source in sources:
        source_name = source.get("name")
        active = source.get("active")
        encoding = source.get("encoding", None)
        if active:
            try:
                logger.info(msg=f"Downloading Source: {source_name}")
                source_type = source.get("type")
                source_download_url = source.get("download_url")
                if source_type == "endpoint":
                    local_filename = f"{source_name}.csv"
                    file_connector.read_and_write_file_locally(
                        source_url=source_download_url,
                        filename=local_filename,
                        encoding=encoding,
                    )
            except Exception as processing_error:
                logger.error(
                    f"Error downloading source data for {source_name}.\n"
                    f"Error: {processing_error}"
                )


def load_and_transform(config, sku_filter):
    all_products_df = pd.DataFrame(columns=FINAL_COLUMNS)
    file_connector = FileConnector()
    sources = config.get("sources", [])
    for source in sources:
        source_name = source.get("name")
        active = source.get("active")
        if active:
            try:
                logger.info(msg=f"Processing Source: {source_name}")
                encoding = source.get("encoding", None)
                local_filename = f"{source_name}.csv"
                file_encoding = "ISO-8859-1" if encoding else "utf-8"
                products_df = file_connector.get_file_df(
                    filename=local_filename, encoding=file_encoding
                )
                if products_df is not None:
                    products_df = map_products(
                        source=source_name, products_df=products_df
                    )
                    products_df = products_df[products_df.sku != 0]
                    products_df = products_df[products_df["sku"].isin(sku_filter)]
                    if not products_df.empty:
                        logger.info(
                            f"Found {len(products_df)} new product updates form {source_name}."
                        )
                        all_products_df = pd.concat(
                            [all_products_df, products_df], ignore_index=True
                        )
            except Exception as processing_error:
                tb = traceback.format_exc()
                error_message = (
                    f"Error processing data for {source_name}. \n"
                    f"Error: {processing_error}. \n"
                    f"Traceback: {tb}"
                )
                logger.error(error_message)

    all_products_df["stock"] = pd.to_numeric(all_products_df["stock"])
    all_products_df_filtered = all_products_df.groupby("sku", group_keys=False).apply(
        lambda x: x.loc[x.stock.idxmax()]
    )
    all_products_df_filtered["tags"] = [[{"id": 790}]] * len(all_products_df_filtered)
    all_products_df_filtered.sku = all_products_df_filtered.sku.astype(int)
    all_products_df_filtered["status"] = 'pending'

    return all_products_df_filtered


def send_products_to_woo(all_new_products_df: pd.DataFrame):
    if not all_new_products_df.empty:
        woo_connector = WooConnector()
        all_existing_products_df = woo_connector.get_products_df()
        all_existing_products_df.sku = (
            pd.to_numeric(all_existing_products_df.sku, errors="coerce")
            .fillna(-1)
            .astype(int)
        )
        products_with_manual_override = all_existing_products_df[
            all_existing_products_df['tags'].apply(lambda tags: {'id': 790, 'name': 'bot', 'slug': 'bot'} not in tags)]

        products_to_be_updated = all_new_products_df[
            all_new_products_df.sku.isin(all_existing_products_df.sku)
        ]
        products_to_be_updated = products_to_be_updated[
            ~products_to_be_updated.sku.isin(products_with_manual_override.sku)
        ]
        products_to_be_created = all_new_products_df[
            ~all_new_products_df.sku.isin(all_existing_products_df.sku)
        ]

        # woo_connector.batch_push_product(
        #     products_to_be_updated.to_json(orient="records"), verb="update"
        # )
        # woo_connector.batch_push_product(
        #     products_to_be_created.to_json(orient="records"), verb="create"
        # )
        _batch_products(all_products_df=products_to_be_updated, woo_connector=woo_connector, verb='update')
        _batch_products(all_products_df=products_to_be_created, woo_connector=woo_connector, verb='create')


def _batch_products(all_products_df, woo_connector, verb='create'):
    all_products_df.reset_index(drop=True, inplace=True)
    batches = math.ceil(len(all_products_df) / BATCH_SIZE)
    for batch in np.array_split(all_products_df, batches):
        if len(batch) > 0:
            woo_connector.batch_push_product(products=batch.to_json(orient="records"), verb=verb)


def delete_all_local_sources(config):
    file_connector = FileConnector()
    sources = config.get("sources", [])
    for source in sources:
        source_name = source.get("name")
        active = source.get("active")
        if active:
            logger.info(msg=f"Processing Source: {source_name}")
            local_filename = f"{source_name}.csv"
            file_connector.delete_local_file(filename=local_filename)


if __name__ == "__main__":
    main()
