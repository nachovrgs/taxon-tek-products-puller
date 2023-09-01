import pandas as pd

from src.connectors.file_connector import FileConnector

if __name__ == "__main__":
    file_c = FileConnector()
    df = file_c.get_file_df(
        filename="ingrammicro.csv",
        encoding="ISO-8859-1",
        separator=",",
        names=[str(x) for x in range(28)],
    )
    print(df)
