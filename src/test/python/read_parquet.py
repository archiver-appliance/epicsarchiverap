import pandas as pd

import click

@click.command()
@click.argument('filename')
def read_file(filename):
    """Simple program that reads filename and prints out statistics."""
    df = pd.read_parquet(filename)
    print(df)

if __name__ == '__main__':
    read_file()

