# IB ACTIVITY STATEMENT IN LOCAL CURRENCY - WORK IN PROGRESS...

IB Activity Statement aims to create Interactive Brokers activity statement in local currency
that is different from account trading base currency for tax reporting purposes

## Input requirements

- download your yearly activity statement in csv format

## Installation and Useage

- download this repo
- create a virtual environment and activate it
- install required packages (pip install -f requirements.txt)
- in the root directory, create a .env file and add "LOCAL_CURRENCY=YOUR_LOCAL_CURRENCY" (e.g. NOK)
- start jupyter notebook server (from command, type "jupyter notebook"), and open the .ipynb notebook
- in the notebook, update the DATA_DIR variable with subdirectory where your activity statement is stored
- also update the DATA_FILE variable with the name of your yearly activity statement csv format file
- lastly update the DATABASE_NAME variable with the name to give to the sqlite database containing the activity statement data

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

- implement missing fields

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
