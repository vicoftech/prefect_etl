from prefect import flow, task
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

INPUT_FILE = "input.csv"
OUTPUT_DIR = "outputs"
Path(OUTPUT_DIR).mkdir(exist_ok=True)

@task
def extract(input_file: str) -> pd.DataFrame:
    df = pd.read_csv(input_file)
    return df

@task
def group_by_jurisdiccion(df: pd.DataFrame) -> pd.DataFrame:
    """Agrupa por jurisdiccion y cuenta registros."""
    grouped = df.groupby('jurisdiccion').size().reset_index(name='cantidad')
    print(grouped)
    return grouped

@task
def plot_jurisdiccion(grouped_df: pd.DataFrame):
    """Genera un gráfico de barras por jurisdiccion."""
    plt.figure(figsize=(6,4))
    plt.bar(grouped_df['jurisdiccion'], grouped_df['cantidad'])
    plt.title('Cantidad por Jurisdicción')
    plt.xlabel('Jurisdicción')
    plt.ylabel('Cantidad')
    plt.tight_layout()
    plt.savefig(Path(OUTPUT_DIR) / "jurisdiccion_plot.png")
    print("🔹 Gráfico guardado en outputs/jurisdiccion_plot.png")

@flow(name="ETL con plot de jurisdiccion")
def etl_flow(input_file: str = INPUT_FILE):
    df = extract(input_file)
    grouped_df = group_by_jurisdiccion(df)
    plot_jurisdiccion(grouped_df)

if __name__ == "__main__":
    etl_flow()
