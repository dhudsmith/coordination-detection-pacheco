import argparse
import pandas as pd

def create_allfeatures_edges(selected_hashtags_edge_file, flagonly_edge_file, output_file):
    """
    Combine the selected_hashtags and flagonly edge files into a single edge file.
    
    Parameters:
    -----------
    selected_hashtags_edge_file : str
        Path to the selected_hashtags edge parquet file
    flagonly_edge_file : str
        Path to the flagonly edge parquet file
    output_file : str
        Path to output the combined edge parquet file
    """
    # Load the edge files
    try:
        hashtags_df = pd.read_parquet(selected_hashtags_edge_file)
        flags_df = pd.read_parquet(flagonly_edge_file)
        
        print(f"Loaded {len(hashtags_df)} hashtag edges for {len(hashtags_df['uid'].unique())} unique authors")
        print(f"Loaded {len(flags_df)} flag edges for {len(flags_df['uid'].unique())} unique authors")
        
        # Combine the dataframes
        combined_df = pd.concat([hashtags_df, flags_df], ignore_index=True)
        
        # Handle any duplicates by summing the cnt column
        combined_df = combined_df.groupby(['uid', 'feature'], as_index=False)['cnt'].sum()
        
        # Ensure data types are consistent
        combined_df['uid'] = combined_df['uid'].astype(str)
        combined_df['feature'] = combined_df['feature'].astype(str)
        combined_df['cnt'] = combined_df['cnt'].astype(int)
        
        # Save the combined edge file
        combined_df.to_parquet(output_file, index=False)
        
        # Also save as CSV for reference
        csv_output = output_file.replace('.parquet', '.csv')
        combined_df.to_csv(csv_output, index=False)
        
        print(f"Created {len(combined_df)} combined edges for {len(combined_df['uid'].unique())} unique authors")
        
    except Exception as e:
        print(f"Error combining edge files: {e}")
        raise

def main():
    parser = argparse.ArgumentParser(description="Combine selected_hashtags and flagonly edge files")
    parser.add_argument("--selected-hashtags", required=True, help="Input selected_hashtags edge parquet file")
    parser.add_argument("--flagonly", required=True, help="Input flagonly edge parquet file")
    parser.add_argument("-o", "--output", required=True, help="Output combined edge parquet file")
    
    args = parser.parse_args()
    
    create_allfeatures_edges(
        args.selected_hashtags,
        args.flagonly,
        args.output
    )

if __name__ == "__main__":
    main()