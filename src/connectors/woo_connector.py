import json

import pandas as pd
from woocommerce import API

from src.logging import logger
from src.settings import Config


class WooConnector:
    """
    Handles the communication to the Woo API
    """

    def __init__(self):
        self.page_size = 100
        self.wcapi = API(
            url=Config.WCAPI_URL,
            consumer_key=Config.ACCESS_KEY_ID,
            consumer_secret=Config.ACCESS_KEY,
            timeout=500,
            version="wc/v3",
        )

    def push_product(self, product):
        self.wcapi.put(f"products/{product['id']}", product)

    def batch_push_product(self, products, verb="create"):
        products_json = json.loads(products)
        data = {verb: products_json}
        result = self.wcapi.post(f"products/batch", data)
        return result

    def get_product(self, product_id):
        product = self.wcapi.get(f"products/{product_id}")
        if product.ok:
            return product.json()
        return None

    def get_products_df(self):
        products = self._get_products_all_pages()
        products_df = pd.DataFrame(products)
        return products_df

    def _get_products_all_pages(self):
        page = 1
        all_products = []
        while True:
            products = self.wcapi.get(
                "products", params={"per_page": self.page_size, "page": page}
            )
            if products.status_code == 200:
                products = products.json()
                if len(products) == 0:  # no more products
                    break
                all_products.extend(products)
                page += 1
            else:
                raise Exception(f"Error getting products. {products.reason}")
        return all_products
