import argparse
import logging
import pandas as pd

from recon import Recon  # assuming recon.py is in the same directory

logging.basicConfig(level=logging.INFO)


def parse_arguments():
    parser = argparse.ArgumentParser(description='CLI tool for csv reconciliation')
    parser.add_argument('--source', '-s', type=str, required=True, help='path/to/the/source.csv')
    parser.add_argument('--target', '-t', type=str, required=True, help='path/to/the/target.csv file')
    parser.add_argument('--output', '-o', type=str, help='Path to the output CSV file', default='cred_csv/data/output.csv')
    parser.add_argument('--columns', '-c', type=str, help='Comma-separated list of columns.')
    return parser.parse_args()


def main():
    args = parse_arguments()
    recon = Recon(args.source, args.target, args.output)
    source_df = recon.get_source_df()
    target_df = recon.get_target_df()

    logging.info("-----> Starting reconciliation...")
    records_not_in_target = recon.get_missing_records(target_df, source_df)
    records_not_in_source = recon.get_missing_records(source_df, target_df)
    columns = recon.get_all_columns()

    if args.columns is not None:
        columns = args.columns.split(',')

    column_differences = recon.compare_columns(columns)

    logging.info("Reconciliation completed:")
    logging.info(f"- Records not in target: {records_not_in_target.shape[0]}")
    logging.info(f"- Records not in source: {records_not_in_source.shape[0]}")
    logging.info(f"- Records with field discrepancies: {column_differences.shape[0]}")

    result_df = pd.concat([records_not_in_source, records_not_in_target, column_differences])
    if args.output:
        result_df.to_csv(args.output, index=False)
        logging.info(f"\nResults saved to: {args.output}")


if __name__ == '__main__':
    main()