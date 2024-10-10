import pandas as pd

class DataLoader:
    @staticmethod
    def load_csv(file_path: str) -> pd.DataFrame:
        """Load a CSV file into a DataFrame."""
        try:
            return pd.read_csv(file_path, encoding='ISO-8859-1',low_memory=False)
        except Exception as e:
            raise FileNotFoundError(f"Error loading {file_path}: {e}")

    @staticmethod
    def load_datasets(data_files: list) -> tuple:
        """Load all datasets from the provided list of CSV file paths."""
        data_frames = []
        for file_path in data_files:
            data_frames.append(DataLoader.load_csv(file_path))
        return tuple(data_frames)
