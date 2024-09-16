import streamlit as st
import duckdb
import pandas as pd

query = "SELECT * FROM 'data\\measurements_summary.parquet'"

# Function to load data from the parquet file
def load_data():
    con = duckdb.connect()
    df = con.execute(query).df()
    con.close()
    return df

# Main function to create dashboard
def main():
    st.title("Weather Station Summary")
    st.write("This dashboard shows the summary of Weather Stations")
    
    # Load data
    data = load_data()
    
    # Show data in table format
    st.dataframe(data)
    
if __name__ == "__main__":
    main()
    