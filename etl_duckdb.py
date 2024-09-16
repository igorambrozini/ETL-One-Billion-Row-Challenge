import duckdb
import time

query = """SELECT
    station,
    min (temperature) as Min_temperature,
    avg (temperature) as Mean_temperature,
    max (temperature) as Max_temperature
FROM read_csv("data\\measurements.txt", AUTO_DETECT=FALSE, sep=';', columns={'station':VARCHAR, 'temperature': 'DECIMAL(3,1)'})

GROUP BY station
ORDER BY station"""

def create_duckcb():
    result = duckdb.sql(query)
    
    result.show()
    
    # Save the result to a Paquet file
    result.write_parquet("data\\measurements_summary.parquet")
    
if __name__ == "__main__":
    import time
    start_time = time.time()
    create_duckcb()
    took = time.time() - start_time
    print(f"Duckdb Took: {took:.2f} sec")