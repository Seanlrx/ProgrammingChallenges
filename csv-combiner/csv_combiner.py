import pandas as pd
import sys
import os


def merge_csv_files(csv_files, chunk_size=100000):
    """
    Combine the input csv files into one csv file, along with an additional column
    that has the filename from which the row came.
    :param csv_files: List of csv file paths.
    :param chunk_size: The maximum number of rows to read into memory at a time.
    """

    if len(csv_files) == 0:
        print("Error: No file to combine.", file=sys.stderr)
        return

    # header indicator: only the first chunk should add the header
    header = True
    for file in csv_files:
        try:
            file_name = os.path.basename(file)
            # read data in chunks to avoid loading too much data into memory
            for chunk in pd.read_csv(file, chunksize=chunk_size):
                # add filename column to the new combined file
                chunk["filename"] = file_name
                print(chunk.to_csv(index=False, header=header, lineterminator='\n', chunksize=chunk_size), end='')
                header = False
        except Exception as e:
            print(f"Error reading file {file}: {e}", file=sys.stderr)


if __name__ == "__main__":
    files = sys.argv[1:]
    merge_csv_files(files)
