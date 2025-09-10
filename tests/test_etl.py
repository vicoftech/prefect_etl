import sys, os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from etl_prefect import group_by_jurisdiccion
import pandas as pd


def test_plot_jurisdiccion():
    df = pd.DataFrame([
        {"name": "Alice", "age": 34, "jurisdiccion": "Buenos Aires"},
        {"name": "Bob", "age": 20, "jurisdiccion": "Cordoba"},
    ])
    outputs = group_by_jurisdiccion.fn(df)  # .fn ejecuta la función del task
    print(outputs.head())
    assert 'jurisdiccion' in outputs
    