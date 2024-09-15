import pandas as pd
from multiprocessing import Pool, cpu_count
from tqdm import tqdm  # imports tqdm for progress bar

CONCURRENCY = cpu_count()

total_lines = 1_000_000_000  # Known total number of lines
chunksize = 100_000_000  # Defines the chunk size
filename = "data\\measurements.txt"  # Make sure this is the correct path to the file

def process_chunk(chunk):
    # Aggregates the data within the chunk using Pandas
    aggregated = chunk.groupby('station')['measure'].agg(['min', 'max', 'mean']).reset_index()
    return aggregated

def create_df_with_pandas(filename, total_lines, chunksize=chunksize):
    total_chunks = total_lines // chunksize + (1 if total_lines % chunksize else 0)
    results = []

    with pd.read_csv(filename, sep=';', header=None, names=['station', 'measure'], chunksize=chunksize) as reader:
        # Wrapping the iterator with tqdm to visualize the progress
        with Pool(CONCURRENCY) as pool:
            for chunk in tqdm(reader, total=total_chunks, desc="Processing"):
                # Processes each chunk in parallel
                result = pool.apply_async(process_chunk, (chunk,))
                results.append(result)

            results = [result.get() for result in results]

    final_df = pd.concat(results, ignore_index=True)

    final_aggregated_df = final_df.groupby('station').agg({
        'min': 'min',
        'max': 'max',
        'mean': 'mean'
    }).reset_index().sort_values('station')

    return final_aggregated_df

if __name__ == "__main__":
    import time

    print("Starting the file processing.")
    start_time = time.time()
    df = create_df_with_pandas(filename, total_lines, chunksize)
    took = time.time() - start_time

    print(df.head())
    print(f"Processing took: {took:.2f} sec")