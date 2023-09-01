import traceback

import pandas as pd

from src.logging.logger import get_module_logger

logger = get_module_logger(__name__)

FINAL_COLUMNS = [
    "sku",
    "name",
    "description",
    "stock",
    "categories",
    "regular_price",
    "images",
]


def map_products(
    source: str, products_df: pd.DataFrame, config
) -> pd.DataFrame:
    try:
        if source == "mcr":
            return map_mcr(products_df=products_df, config=config)
        elif source == "megasur":
            return map_megasur(products_df=products_df, config=config)
        elif source == "impexopcion":
            return map_inpex(products_df=products_df, config=config)
        elif source == "ingrammicro":
            return map_ingrammicro(products_df=products_df, config=config)
        elif source == "supercomp":
            return map_supercomp(products_df=products_df, config=config)
        elif source == "globomatik":
            return map_globo(products_df=products_df, config=config)
        elif source == "bts":
            return map_bts(products_df=products_df, config=config)
        return pd.DataFrame()
    except Exception as error:
        tb = traceback.format_exc()
        error_message = (
            f"Unexpected error while mapping data for {source}. \n"
            f"Error: {error}. \n"
            f"Traceback: {tb}"
        )
        logger.error(error_message)


def map_mcr(products_df: pd.DataFrame, config) -> pd.DataFrame:
    tax = config.get("tax", 0.0)
    products_df = products_df.rename(
        columns={
            "EAN": "sku",
            "Nombre": "name",
            "Descripcion": "description",
            "Stock": "stock",
            "Precio": "regular_price_base",
            "Imagen": "images",
        }
    )
    products_df["sku"] = products_df["sku"].astype(str)
    products_df["categories"] = products_df.apply(
        lambda x: [
            {"id": x["idCategoria1"], "name": x["categoria1"]},
            {"id": x["idCategoria2"], "name": x["categoria2"]},
            {"id": x["idCategoria3"], "name": x["categoria3"]},
        ],
        axis=1,
    )

    products_df = treat_price(products_df, "regular_price_base")

    products_df["regular_price"] = products_df["regular_price_base"] * tax
    products_df.drop(["regular_price_base"], axis=1, inplace=True)
    products_df.images = products_df.images.apply(lambda s: [{"src": s}])

    return products_df[FINAL_COLUMNS]


def map_megasur(products_df: pd.DataFrame, config) -> pd.DataFrame:
    tax = config.get("tax", 0.0)
    products_df = products_df.rename(
        columns={
            "EAN": "sku",
            "NAME": "name",
            "DESCRIPTION": "description",
            "STOCK_DISPONIBLE": "stock",
            "PVD": "regular_price_base",
            "CANON": "regular_price_canon",
            "URL_IMG": "images",
        }
    )
    products_df["sku"] = products_df["sku"].astype(str)
    products_df["categories"] = products_df.apply(
        lambda x: [
            {"id": x["ID_FAMILIA"], "name": x["FAMILIA"]},
            {"id": x["ID_SUBFAMILIA"], "name": x["SUBFAMILIA"]},
        ],
        axis=1,
    )

    products_df = treat_price(products_df, "regular_price_base")
    products_df = treat_price(products_df, "regular_price_canon")

    products_df["regular_price"] = (
        products_df["regular_price_base"] + products_df["regular_price_canon"]
    ) * tax
    products_df.drop(
        ["regular_price_base", "regular_price_base"], axis=1, inplace=True
    )

    products_df.images = products_df.images.apply(lambda s: [{"src": s}])

    return products_df[FINAL_COLUMNS]


def map_bts(products_df: pd.DataFrame, config) -> pd.DataFrame:
    products_df = products_df.rename(
        columns={
            "EAN": "sku",
            "Nombre": "name",
            "Descripcion": "description",
            "Stock": "stock",
            "Precio": "regular_price",
            "Imagen": "images",
        }
    )
    products_df["sku"] = products_df["sku"].astype(str)
    products_df["categories"] = products_df.apply(
        lambda x: [
            {"id": x["idCategoria1"], "name": x["categoria1"]},
            {"id": x["idCategoria2"], "name": x["categoria2"]},
            {"id": x["idCategoria3"], "name": x["categoria3"]},
            {"id": x["idCategoria4"], "name": x["categoria4"]},
        ],
        axis=1,
    )

    products_df.images = products_df.images.apply(lambda s: [{"src": s}])

    return products_df[FINAL_COLUMNS]


def map_supercomp(products_df: pd.DataFrame, config) -> pd.DataFrame:
    tax = config.get("tax", 0.0)
    products_df = products_df.rename(
        columns={
            "EAN": "sku",
            "NOMBREARTICULO": "name",
            "DESCRIPCION": "description",
            "STOCK": "stock",
            "PRECIO": "regular_price_base",
            "CANON": "regular_price_canon",
            "IMAGEN": "images",
        }
    )
    products_df["sku"] = products_df["sku"].astype(str)
    products_df["categories"] = products_df.apply(
        lambda x: [{"id": x["IDCATEGORIA"], "name": x["CATEGORIA"]}], axis=1
    )

    products_df = treat_price(products_df, "regular_price_base")
    products_df = treat_price(products_df, "regular_price_canon")

    products_df["regular_price"] = (
        products_df["regular_price_base"] + products_df["regular_price_canon"]
    ) * tax
    products_df.drop(
        ["regular_price_base", "regular_price_base"], axis=1, inplace=True
    )
    products_df.images = products_df.images.apply(lambda s: [{"src": s}])
    return products_df[FINAL_COLUMNS]


def map_globo(products_df: pd.DataFrame, config) -> pd.DataFrame:
    tax = config.get("tax", 0.0)
    products_df = products_df.rename(
        columns={
            "EAN": "sku",
            "Desc. Comercial": "name",
            "Desc. Larga": "description",
            "Stock": "stock",
            "Precio": "regular_price_base",
            "Canon": "regular_price_canon",
            "Imagen": "images",
        }
    )
    products_df["sku"] = products_df["sku"].astype(str)
    products_df["categories"] = products_df.apply(
        lambda x: [
            {"name": x["Familia"]},
            {"name": x["SubFamilia"]},
        ],
        axis=1,
    )

    products_df = treat_price(products_df, "regular_price_base")
    products_df = treat_price(products_df, "regular_price_canon")

    products_df["regular_price"] = (
        products_df["regular_price_base"] + products_df["regular_price_canon"]
    ) * tax
    products_df.drop(
        ["regular_price_base", "regular_price_base"], axis=1, inplace=True
    )
    products_df.images = products_df.images.apply(lambda s: [{"src": s}])

    return products_df[FINAL_COLUMNS]


def map_inpex(products_df: pd.DataFrame, config) -> pd.DataFrame:
    products_df = products_df.rename(
        columns={
            "ean": "sku",
            "name": "name",
            "stock_total": "stock",
            "precio_con_iva": "regular_price",
        }
    )
    products_df["sku"] = products_df["sku"].astype(str)
    products_df["description"] = products_df["name"]
    products_df["categories"] = products_df.apply(
        lambda x: [{"name": x["category"]}], axis=1
    )

    products_df["images"] = ""

    return products_df[FINAL_COLUMNS]


def map_ingrammicro(products_df: pd.DataFrame, config) -> pd.DataFrame:
    tax = config.get("tax", 0.0)
    products_df = products_df.rename(
        columns={
            "0": "sku",
            "5": "name",
            "6": "description",
            "22": "stock",
            "23": "regular_price_base",
        }
    )
    products_df["sku"] = products_df["sku"].astype(str)
    products_df["categories"] = products_df.apply(
        lambda x: [{"name": x["1"]}], axis=1
    )

    products_df = treat_price(products_df, "regular_price_base")

    products_df["regular_price"] = (
        products_df["regular_price_base"].astype(float, errors="ignore") * tax
    )
    products_df.drop(["regular_price_base"], axis=1, inplace=True)

    products_df["images"] = ""

    return products_df[FINAL_COLUMNS]


def treat_price(products_df: pd.DataFrame, column_name) -> pd.DataFrame:
    products_df[column_name] = (
        products_df[column_name].astype(str).str.replace(".", "")
    )
    products_df[column_name] = (
        products_df[column_name].astype(str).str.replace(",", ".")
    )
    products_df[column_name] = products_df[column_name].astype(
        float, errors="ignore"
    )
    return products_df
