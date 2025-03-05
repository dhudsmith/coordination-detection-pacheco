import argparse
import pandas as pd

def create_flagonly_edges(input_file, output_file, flags):
    # Load authors CSV file
    df = pd.read_csv(input_file)
    
    # Standardize column names - ensure we have author_id
    if 'handle' in df.columns and 'author_id' not in df.columns:
        df.rename(columns={'handle': 'author_id'}, inplace=True)
    
    # Ensure flags are present in the dataframe
    available_flags = [col for col in df.columns if col in flags]
    if not available_flags:
        raise ValueError(f"None of the requested flags {flags} found in the input file")
    
    # Create a list to store edges
    edges = []
    
    # For each author, create an edge for each flag that is True/1
    for _, row in df.iterrows():
        author_id = row['author_id']
        for flag in available_flags:
            # Check if the flag value is true (1, True, etc.)
            if pd.notna(row[flag]) and (row[flag] == 1 or row[flag] is True or str(row[flag]).lower() == 'true'):
                # Add an edge (author_id, flag, 1)
                edges.append((author_id, flag, 1))
    
    # Create the edge dataframe
    edge_df = pd.DataFrame(edges, columns=['uid', 'feature', 'cnt'])
    
    # Save as parquet
    edge_df['uid'] = edge_df['uid'].astype(str)
    edge_df['feature'] = edge_df['feature'].astype(str)
    edge_df['cnt'] = edge_df['cnt'].astype(int)  # Make sure cnt remains an integer
    edge_df.to_parquet(output_file, index=False)
    
    # Also save as CSV
    csv_output = output_file.replace('.parquet', '.csv')
    edge_df.to_csv(csv_output, index=False)
    
    print(f"Created {len(edge_df)} edges for {len(edge_df['uid'].unique())} unique authors")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create edge files based on author flags")
    parser.add_argument("-i", "--input", required=True, help="Input authors CSV file")
    parser.add_argument("-o", "--output", required=True, help="Output parquet file")
    parser.add_argument("-f", "--flags", nargs='+', required=True, help="List of flag columns to use")
    
    args = parser.parse_args()
    
    create_flagonly_edges(args.input, args.output, args.flags)