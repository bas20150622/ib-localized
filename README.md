# IB ACTIVITY STATEMENT IN LOCAL CURRENCY - WORK IN PROGRESS...

- calculate and output balances per account
- calcluate and output profit and loss per account
- calculate and output net dividends (dividends - witholding tax) per account

IB Activity Statement aims to create Interactive Brokers activity statement in local currency
that is different from account trading base currency for tax reporting purposes

## Input requirements

- download your yearly (multi-account) activity statement in csv format

## Installation and Useage

- download this repo
- create a virtual environment and activate it
- install required packages (pip install -r requirements.txt)
- in the root directory, create a .env file and add "LOCAL_CURRENCY=YOUR_LOCAL_CURRENCY" (e.g. NOK)
- start jupyter notebook server (from command, type "jupyter notebook"), and open the .ipynb notebook
- in the notebook, update the DATA_DIR variable with subdirectory where your activity statement is stored
- also update the DATA_FILE variable with the name of your yearly activity statement csv format file
- lastly update the DATABASE_NAME variable with the name to give to the sqlite database containing the activity statement data

## Current implementation limitation

- Only Norwegian forex vs EUR / USD / GBP is implemented - see forex.py. Pls extend to implement any other currencies
- Implemented data types (i.e. csv row header types):
  - Statement
  - Account Information
  - Net Asset Value (note1)
  - Open Positions
  - Trades
  - Forex Balances
  - Deposits & Withdrawals
  - Dividends
  - Withholding Tax
  - Change in Dividend Accruals
  - Account Summary
  - Cash Report
  - Transfers
  - Mark-to-Market Performance Summary
- Not implemented data types:
  - Change in NAV
  - Total P/L for Statement Period **(not required)**
  - Realized & Unrealized Performance Summary
  - Corporate Actions: **Asset Category,Account, Currency,Report Date,Date/Time,Description,Quantity,Proceeds,Value,Realized P/L,Code**
  - Fees
  - Financial Instrument Information
  - Codes
  - Notes/Legal Notes
  - Other
  - IBKR Managed Securities Collateral Held at IBSS (Stock Yield Enhancement Program)
  - Interest
  - Interest Accruals
  - Location of Customer Assets, Positions and Money
Note 1: only first isolated header - non unique located repeat headers are ignored









- If a same data type switches from header to data back to header, the latter headers and data are **ignored**


## Key Modules

- csv_parser: contains csv file processing algorithms
- forex.py: contains forex rate retriever functionality
- sqla.py: contains the sqla orm models for database tables
- .env: should contain LOCAL_CURRENCY definition
- settings.py: methods for reading local settings
- db.py: database access class definition
- xcoder.py: encoders relevant for schemas/ orm models
- enums.py: enum definitions for schemas/ orm models
- queries: db queries providing necessary output
- TODO add ipynb notebook

## Implementation details

- rows in the csv format file are read and parsed one by one, into data type specific
  data dictionaries.
- data dictionaries containing DateTime keys are updated with a forex quote in local currency
- each dict is converted to database orm model and added to db
- Statements and Account Information are generalized and mapped to NameValue models
- Account Summary is mapped to Account models
