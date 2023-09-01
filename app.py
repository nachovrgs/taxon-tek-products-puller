import asyncio
import math
import sys
import time
import traceback
from datetime import datetime

import numpy as np
import pandas as pd
import pytz

sys.path.append("/home/aukiozgq/order_puller/")
from src.connectors.config_connector import ConfigConnector
from src.connectors.file_connector import FileConnector
from src.connectors.woo_connector import WooConnector
from src.logging.logger import get_module_logger
from src.mappers import FINAL_COLUMNS, map_products
from src.settings import Config

logger = get_module_logger(__name__)
BATCH_SIZE = 100

FINAL_COLUMNS = [
    "sku",
    "name",
    "description",
    "stock",
    "categories",
    "regular_price",
    "images",
]


async def main():
    """
    Main handler for the Puller.
    Gets all new products and pushes them into Woo
    :return:
    """

    try:
        config_connector = ConfigConnector()
        config = config_connector.get_config()
        dryrun = config.get("dryrun", True)
        use_local = config.get("use_local", False)
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

        if not use_local:
            await download_all_sources(config=config)

        logger.info(msg="---- Downloading sources is done.")

        all_products_df = load_and_transform(
            config=config, sku_filter=sku_filter
        )
        logger.info(msg="---- Mapping products is done.")
        if not dryrun:
            send_products_to_woo(all_new_products_df=all_products_df)

            logger.info(msg="---- Writing updated config.")
            config["last_check"] = now
            config_connector.set_config(config)
        else:
            all_products_df.to_csv(f"tmp/all_products.csv")

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
    logger.info(msg="---- Puller finished")
    logger.info(msg="")


def load_sku_filter(config):
    doc_url = config.get("skus_doc_url")
    sheet_name = config.get("skus_doc_sheet_name").replace(" ", "%20")
    csv_url = f"{doc_url}/export?gid=0&format=csv&sheet={sheet_name}"
    df = pd.read_csv(csv_url, header=None, on_bad_lines="skip")
    df.columns = ["sku"]
    df["sku"] = df["sku"].astype(str)
    return df.sku.to_list()


async def download_all_sources(config):
    tasks = []
    file_connector = FileConnector()
    sources = config.get("sources", [])
    for source in sources:
        task = asyncio.create_task(
            download_single_source(source, file_connector)
        )
        tasks.append(task)
    await asyncio.gather(*tasks)


async def download_single_source(source, file_connector):
    use_local = source.get("use_local", False)
    if not use_local:
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
                elif source_type == "ftp":
                    file_path = source.get("file_path", None)
                    user = source.get("user", None)
                    password = source.get("password", None)
                    expected_file = source.get("expected_file", None)
                    if file_path and user and password and expected_file:
                        file_connector.read_and_write_ftp_locally(
                            source_url=source_download_url,
                            file_path=file_path,
                            filename=source_name,
                            expected_file=expected_file,
                            user=user,
                            password=password,
                        )

            except Exception as processing_error:
                logger.error(
                    f"Error downloading source data for {source_name}.\n"
                    f"Error: {processing_error}"
                )


def load_and_transform(config, sku_filter):
    dfs_to_concat = []
    all_existing_skus = {}
    all_data_sources = {}
    file_connector = FileConnector()
    sources = config.get("sources", [])
    differential_price = config.get("differential_price", {})
    for source in sources:
        source_name = source.get("name")
        active = source.get("active")
        if active:
            try:
                logger.info(msg=f"---- Processing Source: {source_name} ----")
                local_filename = f"{source_name}.csv"
                separator = source.get("separator", ";")
                header = source.get("header", None)
                names = source.get("names", None)
                engine = source.get("engine", "python")
                if source_name == "ingrammicro":
                    names = [str(x) for x in range(28)]
                encoding = source.get("encoding", None)
                file_encoding = "ISO-8859-1" if encoding else "utf-8"
                products_df = file_connector.get_file_df(
                    filename=local_filename,
                    encoding=file_encoding,
                    separator=separator,
                    header=header,
                    names=names,
                    engine=engine,
                )
                if products_df is not None:
                    logger.info(
                        f"Got {len(products_df)} products from {source_name} before mapping"
                    )
                    products_df = map_products(
                        source=source_name,
                        products_df=products_df,
                        config=config,
                    )
                    if products_df is not None:
                        logger.info(
                            f"{len(products_df)} products remain from {source_name} after initial mapping"
                        )
                        products_df = products_df[products_df.stock > 0]
                        logger.info(
                            f"{len(products_df)} products remain from {source_name} after stock filter"
                        )
                        products_df = products_df[
                            products_df["sku"].isin(sku_filter)
                        ]
                        logger.info(
                            f"{len(products_df)} products remain from {source_name} after sku filter"
                        )
                        all_data_sources[source_name] = products_df
                        if not products_df.empty:
                            existing_skus = products_df["sku"].to_list()
                            for existing_sku in existing_skus:
                                str_existing_sku = str(existing_sku)
                                if str_existing_sku in all_existing_skus:
                                    all_existing_skus[str_existing_sku].append(
                                        source_name
                                    )
                                else:
                                    all_existing_skus[str_existing_sku] = [
                                        source_name
                                    ]
                            logger.info(
                                f"Found {len(products_df)} new product updates form {source_name}."
                            )
                            dfs_to_concat.append(products_df)
            except Exception as processing_error:
                tb = traceback.format_exc()
                error_message = (
                    f"Error processing data for {source_name}. \n"
                    f"Error: {processing_error}. \n"
                    f"Traceback: {tb}"
                )
                logger.error(error_message)

    def extract_name(row):
        sku = row["sku"]
        name = None
        if sku and (not isinstance(sku, str) and not math.isnan(sku)):
            sources_that_have_the_sku = all_existing_skus[sku]
            defaults = config.get("defaults", [])
            default_name_sources = defaults.get("name", ["source"])
            for default_name_source in default_name_sources:
                if default_name_source == "source":
                    for source in sources_that_have_the_sku:
                        values = all_data_sources[source][
                            all_data_sources[source]["sku"] == sku
                        ].iloc[0]
                        if values["name"]:
                            return values["name"]
                elif default_name_source in sources_that_have_the_sku:
                    values = all_data_sources[default_name_source][
                        all_data_sources[default_name_source]["sku"] == sku
                    ].iloc[0]
                    if values["name"]:
                        return values["names"]
        return name

    def extract_description(row):
        sku = row["sku"]
        description = None
        if sku and (not isinstance(sku, str) and not math.isnan(sku)):
            sources_that_have_the_sku = all_existing_skus[sku]
            defaults = config.get("defaults", [])
            default_description_sources = defaults.get(
                "description", ["source"]
            )
            for default_description_source in default_description_sources:
                if default_description_source == "source":
                    for source in sources_that_have_the_sku:
                        values = all_data_sources[source][
                            all_data_sources[source]["sku"] == sku
                        ].iloc[0]
                        if values["description"]:
                            return values["description"]
                elif default_description_source in sources_that_have_the_sku:
                    values = all_data_sources[default_description_source][
                        all_data_sources[default_description_source]["sku"]
                        == sku
                    ].iloc[0]
                    if values["description"]:
                        return values["description"]
        return description

    def extract_price(row):
        sku = row["sku"]
        prices = []
        final_price = None
        if sku and (not isinstance(sku, str) and not math.isnan(sku)):
            sources_that_have_the_sku = all_existing_skus[sku]
            for source_that_has_the_sku in sources_that_have_the_sku:
                values = all_data_sources[source_that_has_the_sku][
                    all_data_sources[source_that_has_the_sku]["sku"] == sku
                ].iloc[0]
                prices.append(values["regular_price"])
            min_price = min(prices)
            final_price = min_price
            for price_range, multiplier in differential_price.items():
                if price_range == "inf":
                    return final_price * multiplier
                elif final_price <= int(price_range):
                    return final_price * multiplier
        return final_price

    def extract_stock(row):
        """
        Based on the price, we first apply the same logic as the price and get that lowest price stock
        """
        sku = row["sku"]
        stock = 0

        if sku and (not isinstance(sku, str) and not math.isnan(sku)):
            prices = {}
            sources_that_have_the_sku = all_existing_skus[sku]
            for source_that_has_the_sku in sources_that_have_the_sku:
                values = all_data_sources[source_that_has_the_sku][
                    all_data_sources[source_that_has_the_sku]["sku"] == sku
                ].iloc[0]
                prices[source_that_has_the_sku] = values["regular_price"]

            min_price = min(prices.values())

            chosen_source = None
            for key, value in prices.items():
                if value == min_price:
                    chosen_source = key
            if chosen_source:
                values = all_data_sources[chosen_source][
                    all_data_sources[chosen_source]["sku"] == sku
                ].iloc[0]
                stock = values["stock"]
        return stock

    def extract_categories(row):
        """
        Based on the price, we first apply the same logic as the price and get that lowest price categories
        """
        sku = row["sku"]
        categories = []

        if sku and (not isinstance(sku, str) and not math.isnan(sku)):
            prices = {}
            sources_that_have_the_sku = all_existing_skus[sku]
            for source_that_has_the_sku in sources_that_have_the_sku:
                values = all_data_sources[source_that_has_the_sku][
                    all_data_sources[source_that_has_the_sku]["sku"] == sku
                ].iloc[0]
                prices[source_that_has_the_sku] = values["regular_price"]

            min_price = min(prices.values())

            chosen_source = None
            for key, value in prices.items():
                if value == min_price:
                    chosen_source = key
            if chosen_source:
                values = all_data_sources[chosen_source][
                    all_data_sources[chosen_source]["sku"] == sku
                ].iloc[0]
                categories = values["categories"]
        return categories

    def extract_images(row):
        sku = row["sku"]

        if sku and (not isinstance(sku, str) and not math.isnan(sku)):
            sources_that_have_the_sku = all_existing_skus[sku]
            defaults = config.get("defaults", [])
            default_images_sources = defaults.get("images", ["source"])
            for default_images_source in default_images_sources:
                if default_images_source == "source":
                    return row["images"]
                elif default_images_source in sources_that_have_the_sku:
                    values = all_data_sources[default_images_source][
                        all_data_sources[default_images_source]["sku"] == sku
                    ].iloc[0]
                    return values["images"]
        return None

    logger.info(
        f"{len(all_existing_skus.keys())} unique products remain after looking at all sources. \n"
        f"Starting the merge process."
    )

    final_df = pd.DataFrame(columns=FINAL_COLUMNS)
    final_df["sku"] = all_existing_skus.keys()

    final_df["name"] = final_df.apply(extract_name, axis=1)
    final_df["description"] = final_df.apply(extract_description, axis=1)
    final_df["stock"] = final_df.apply(extract_stock, axis=1)
    final_df["categories"] = final_df.apply(extract_categories, axis=1)
    final_df["regular_price"] = final_df.apply(extract_price, axis=1)
    final_df["images"] = final_df.apply(extract_images, axis=1)

    if final_df is not None:
        treated_all_products_df = treat_all_products_df(
            all_products_df=final_df
        )
    else:
        treated_all_products_df = final_df

    logger.info(
        f"{len(treated_all_products_df)} unique products remain after merge of all sources."
    )
    return treated_all_products_df


def treat_all_products_df(all_products_df: pd.DataFrame) -> pd.DataFrame:
    all_products_df["stock"] = pd.to_numeric(all_products_df["stock"])
    all_products_df.to_csv("tmp/filtered_df.csv")

    default_tags_to_add = [{"id": 790}]
    columns_to_drop = ["source"]

    all_products_df["tags"] = [default_tags_to_add] * len(all_products_df)
    all_products_df.sku = all_products_df.sku.astype(int)
    all_products_df["status"] = "pending"
    # all_products_df_filtered.drop(columns_to_drop, axis=1, inplace=True)
    return all_products_df


def send_products_to_woo(all_new_products_df: pd.DataFrame):
    if not all_new_products_df.empty:
        logger.info(
            f"Finished processing everything. Got a total of {len(all_new_products_df)} products."
        )
        woo_connector = WooConnector()
        all_existing_products_df = woo_connector.get_products_df()
        all_existing_products_df.sku = (
            pd.to_numeric(all_existing_products_df.sku, errors="coerce")
            .fillna(-1)
            .astype(int)
        )
        products_with_manual_override = all_existing_products_df[
            all_existing_products_df["tags"].apply(
                lambda tags: {"id": 790, "name": "bot", "slug": "bot"}
                not in tags
            )
        ]

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
        if not products_to_be_updated.empty:
            logger.info(
                f"Will send {len(products_to_be_updated)} product updates to woo."
            )
            _batch_products(
                all_products_df=products_to_be_updated,
                woo_connector=woo_connector,
                verb="update",
            )
        if not products_to_be_created.empty:
            logger.info(
                f"Will send {len(products_to_be_created)} new products to woo."
            )
            _batch_products(
                all_products_df=products_to_be_created,
                woo_connector=woo_connector,
                verb="create",
            )

        logger.info(f"Finished sending products to woo.")


def _batch_products(all_products_df, woo_connector, verb="create"):
    all_products_df.reset_index(drop=True, inplace=True)
    batches = math.ceil(len(all_products_df) / BATCH_SIZE)
    for batch in np.array_split(all_products_df, batches):
        if len(batch) > 0:
            woo_connector.batch_push_product(
                products=batch.to_json(orient="records"), verb=verb
            )


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
    s = time.perf_counter()
    asyncio.run(main())
    elapsed = time.perf_counter() - s
    print(f"Puller executed in {elapsed:0.2f} seconds.")
