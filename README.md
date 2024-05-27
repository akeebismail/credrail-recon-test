# CREDRAIL CSV Reconciler

### This is cli application that read in two CSV files, reconciles the records, and produce a report detailing the differences between the two.

## Requirements
The following are the app requirements:
- Python 3.12

## Running the cli

1. Install required packages
    `pip install -r requirements.txt`

### Running app in cli
Run the following to run a reconciliation
`python cred_csv/reconciler.py -s path/to/source.csv -t path/to/target.csv -o path/to/output.csv -c columns/to/compare`


### Suggestions

Loading csv file is fundamentally alot of work, which includes:

1. You need to split into lines.
2. You need to split each line on commas.
3. You need to deal with string quoting.
4. You need to guess(!) the types of columns, unless you explicitly pass them to Pandas.
5. You need to convert strings into integers, dates, and other non-string types.

**all of the above tasks take CPU time**

Instead of reading in a CSV, you could read in some other file format that is faster to process. Let’s see an example, using the Parquet data format. Parquet files are designed to be read quickly: you don’t have to do as much parsing as you would with CSV. And unlike CSV, where the column type is not encoded in the file, in Parquet the columns have types stored in the actual file.

Example 
```python
import pandas as pd

df = pd.read_csv("large.csv")
df.to_parquet("large.parquet", compression=None)
```
*the best of csv is no csv*, but If you are stuck with CSV, consider using the new PyArrow CSV parser in Pandas 1.4; you’ll get a nice speed-up, especially if your program is not currently taking advantage of multiple CPUs.