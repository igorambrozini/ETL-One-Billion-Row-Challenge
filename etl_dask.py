import dask
import dask.dataframe as dd

def create_dask_df():
    dask.config.set({'dataframe.query-planning': True})
    
    # Configuring the Dask DataFrame to read the CSV file
    # Since the file has no header, we manually specify the column names
    df = dd.read_csv("data/measurements.txt", sep=";", header=None, names=["station", "measure"])
    
    # Grouping by 'station' and calculating the maximum, minimum, and mean of 'measure'
    # Dask performs operations lazily, so this part only defines the calculation
    grouped_df = df.groupby("station")['measure'].agg(['max', 'min', 'mean']).reset_index()

    # Dask does not efficiently support direct sorting of grouped/resultant DataFrames
    # But you can compute the result and then sort it if the resulting dataset is not too large
    # or if it is essential for the next stage of processing
    # Sorting will be performed after the call to .compute(), if necessary

    return grouped_df

if __name__ == "__main__":
    import time

    start_time = time.time()
    df = create_dask_df()
    
    # The actual computation and sorting are done here
    result_df = df.compute().sort_values("station")
    took = time.time() - start_time

    print(result_df)
    print(f"Dask Took: {took:.2f} sec")
