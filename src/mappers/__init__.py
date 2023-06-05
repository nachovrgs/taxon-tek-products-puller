import pandas as pd
import traceback
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


def map_products(source: str, products_df: pd.DataFrame) -> pd.DataFrame:
    try:
        if source == "mcr":
            return map_mcr(products_df=products_df)
        elif source == "megasur":
            return map_megasur(products_df=products_df)
        elif source == "impexopcion":
            return map_inpex(products_df=products_df)
        elif source == "supercomp":
            return map_supercomp(products_df=products_df)
        elif source == "globomatik":
            return map_globo(products_df=products_df)
        elif source == "bts":
            return map_bts(products_df=products_df)
        return pd.DataFrame()
    except Exception as error:
        tb = traceback.format_exc()
        error_message = (
            f"Unexpected error while mapping data for {source}. \n"
            f"Error: {error}. \n"
            f"Traceback: {tb}"
        )
        logger.error(error_message)


def map_mcr(products_df: pd.DataFrame) -> pd.DataFrame:
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
    products_df["categories"] = products_df.apply(
        lambda x: [
            {"id": x["idCategoria1"], "name": x["categoria1"]},
            {"id": x["idCategoria2"], "name": x["categoria2"]},
            {"id": x["idCategoria3"], "name": x["categoria3"]},
        ],
        axis=1,
    )

    products_df.images = products_df.images.apply(lambda s: [{'src': s}])

    return products_df[FINAL_COLUMNS]


def map_megasur(products_df: pd.DataFrame) -> pd.DataFrame:
    products_df = products_df.rename(
        columns={
            "EAN": "sku",
            "NAME": "name",
            "DESCRIPTION": "description",
            "STOCK_DISPONIBLE": "stock",
            "PVD": "regular_price",
            "URL_IMG": "images",
        }
    )
    products_df["categories"] = products_df.apply(
        lambda x: [
            {"id": x["ID_FAMILIA"], "name": x["FAMILIA"]},
            {"id": x["ID_SUBFAMILIA"], "name": x["SUBFAMILIA"]},
        ],
        axis=1,
    )
    products_df['regular_price'] = products_df['regular_price'].str.replace('.', '')
    products_df['regular_price'] = products_df['regular_price'].str.replace(',', '.')
    products_df['regular_price'] = products_df['regular_price'].astype(float)

    products_df.images = products_df.images.apply(lambda s: [{'src': s}])

    return products_df[FINAL_COLUMNS]


def map_bts(products_df: pd.DataFrame) -> pd.DataFrame:
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
    products_df["categories"] = products_df.apply(
        lambda x: [
            {"id": x["idCategoria1"], "name": x["categoria1"]},
            {"id": x["idCategoria2"], "name": x["categoria2"]},
            {"id": x["idCategoria3"], "name": x["categoria3"]},
            {"id": x["idCategoria4"], "name": x["categoria4"]},
        ],
        axis=1,
    )

    products_df.images = products_df.images.apply(lambda s: [{'src': s}])

    return products_df[FINAL_COLUMNS]


def map_supercomp(products_df: pd.DataFrame) -> pd.DataFrame:
    products_df = products_df.rename(
        columns={
            "EAN": "sku",
            "NOMBREARTICULO": "name",
            "DESCRIPCION": "description",
            "STOCK": "stock",
            "PRECIO": "regular_price",
            "IMAGEN": "images",
        }
    )
    products_df["categories"] = products_df.apply(
        lambda x: [{"id": x["IDCATEGORIA"], "name": x["CATEGORIA"]}], axis=1
    )
    products_df['regular_price'] = products_df['regular_price'].str.replace('.', '')
    products_df['regular_price'] = products_df['regular_price'].str.replace(',', '.')
    products_df['regular_price'] = products_df['regular_price'].astype(float)
    products_df.images = products_df.images.apply(lambda s: [{'src': s}])
    return products_df[FINAL_COLUMNS]


def map_globo(products_df: pd.DataFrame) -> pd.DataFrame:
    products_df = products_df.rename(
        columns={
            "EAN": "sku",
            "Desc. Comercial": "name",
            "Desc. Larga": "description",
            "Stock": "stock",
            "Precio": "regular_price",
            "Imagen": "images",
        }
    )
    products_df["categories"] = products_df.apply(
        lambda x: [
            {"name": x["Familia"]},
            {"name": x["SubFamilia"]},
        ],
        axis=1,
    )

    products_df.images = products_df.images.apply(lambda s: [{'src': s}])

    return products_df[FINAL_COLUMNS]


def map_inpex(products_df: pd.DataFrame) -> pd.DataFrame:
    products_df = products_df.rename(
        columns={
            "ean": "sku",
            "name": "name",
            "stock_total": "stock",
            "precio_con_iva": "regular_price",
        }
    )
    products_df["description"] = products_df["name"]
    products_df["categories"] = products_df.apply(
        lambda x: [{"name": x["category"]}], axis=1
    )

    products_df['images'] = ''

    return products_df[FINAL_COLUMNS]
