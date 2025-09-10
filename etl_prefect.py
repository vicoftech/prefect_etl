from prefect import flow, task
import subprocess
import sys
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# Directorio de salida
INPUT_FILE = "input.csv"
OUTPUT_DIR = "outputs"
Path(OUTPUT_DIR).mkdir(exist_ok=True)

# ----------------------------
# Task para instalar dependencias
# ----------------------------
@task
def install_dependencies():
    """Instala dependencias del proyecto antes de ejecutar el flujo."""
    try:
        subprocess.run([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"], check=True)
        print("✅ Dependencias instaladas correctamente")
    except subprocess.CalledProcessError as e:
        print("❌ Error instalando dependencias:", e)
        raise

# ----------------------------
# ETL Tasks
# ----------------------------
@task
def extract(input_file: str) -> pd.DataFrame:
    df = pd.read_csv(input_file)
    return df

@task
def group_by_jurisdiccion(df: pd.DataFrame) -> pd.DataFrame:
    grouped = df.groupby('jurisdiccion').size().reset_index(name='cantidad')
    print(grouped)
    return grouped

@task
def plot_jurisdiccion(grouped_df: pd.DataFrame):
    plt.figure(figsize=(6, 4))
    plt.bar(grouped_df['jurisdiccion'], grouped_df['cantidad'])
    plt.title('Cantidad por Jurisdicción')
    plt.xlabel('Jurisdicción')
    plt.ylabel('Cantidad')
    plt.tight_layout()
    plt.savefig(Path(OUTPUT_DIR) / "jurisdiccion_plot.png")
    print("🔹 Gráfico guardado en outputs/jurisdiccion_plot.png")

# ----------------------------
# Flow principal
# ----------------------------
@flow(name="ETL con plot de jurisdiccion")
def etl_flow(input_file: str = INPUT_FILE):
    # 1️⃣ Instala dependencias primero
    install_dependencies()
    
    # 2️⃣ ETL
    df = extract(input_file)
    grouped_df = group_by_jurisdiccion(df)
    plot_jurisdiccion(grouped_df)

if __name__ == "__main__":
    etl_flow()
