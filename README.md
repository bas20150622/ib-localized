# IB ACTIVITY STATEMENT IN LOCAL CURRENCY

IB Activity Statement aims to create Interactive Brokers activity statement in local currency for tax reporting
purposes.

## Input requirements

- download your yearly activity statement in csv format

## Useage

- open the ipython notebook and update with path to your yearly activity statement csv format file
- UPDATE DATABASE NAME

## Current implementation limitation

- Only Norwegian forex vs EUR / USD is implemented
- Implemented data types:
  - Statement
  - Account Information
  - Net Asset Value
  - Open Positions
  - Trades
  - Forex Balances
  - Deposits & Withdrawals
  - Dividends
  - Withholding Tax
- Not implemented data types:
  - Change in NAV
  - Mark-to-Market Performance Summary
  - Total P/L for Statement Period **(not required)**
  - Realized & Unrealized Performance Summary
  - Cash Report
  - Corporate Actions
  - Fees
  - Change in Dividend Accruals
  - Financial Instrument Information
  - Codes
  - Notes/Legal Notes
  - Other

## TODO

## Key Modules

- csv_parser: contains csv file processing algorithms
- forex.py: contains forex rate retriever functionality
- sqla.py: contains the sqla orm models for database tables
- .env: should contain LOCAL_CURRENCY definition
- settings.py: methods for reading local settings
- db.py: database access class definition
- xcoder.py: encoders relevant for schemas/ orm models
- enums.py: enum definitions for schemas/ orm models
- TODO add ipynb notebook
- TODO: add queries

## Implementation details

- rows in the csv format file are read and parsed one by one, into data type specific
  data dictionaries.
- data dictionaries containing DateTime keys are updated with a forex quote in local currency
- each dict is converted to database orm model and added to db
