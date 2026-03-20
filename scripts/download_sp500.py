import pandas as pd
from pathlib import Path


def download_sp500():
    url = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"

    print("Downloading S&P 500 constituents...")
    df = pd.read_csv(url)

    # Keep only what we need
    df = df[["Symbol", "Security"]].copy()

    # Add Stooq symbol mapping
    df["stooq_symbol"] = df["Symbol"].str.lower() + ".us"

    # Save to data folder
    data_path = Path(__file__).resolve().parent.parent / "data"
    data_path.mkdir(exist_ok=True)

    output_file = data_path / "sp500_constituents.csv"
    df.to_csv(output_file, index=False)

    print(f"Saved to {output_file}")
    print(f"Total tickers: {len(df)}")
    print(df.head(10))


if __name__ == "__main__":
    download_sp500()