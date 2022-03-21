#!/usr/bin/env python3

import sys
import numpy as np
import pandas as pd
import pyarrow as pa

if len(sys.argv) != 2:
    print("Usage: %s <output table name>" % sys.argv[0])
    sys.exit(1)

df = pd.DataFrame({'s1' : ['string1', 'string2'],
    's2' : ['string3', 'string4']})
table = pa.Table.from_pandas(df)

import pyarrow.parquet as pq
pq.write_table(table, sys.argv[1])
