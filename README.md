# tap-ireckonu

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

## How to use it

`tap-ireckonu` works together with any other [Singer Target](https://singer.io) to move data from the iReckonu API to any target destination.

### Install

First, make sure Python 3 is installed on your system or follow these
installation instructions for [Mac](http://docs.python-guide.org/en/latest/starting/install3/osx/) or
[Ubuntu](https://www.digitalocean.com/community/tutorials/how-to-install-python-3-and-set-up-a-local-programming-environment-on-ubuntu-16-04).

This project is set up using [Python Poetry](https://python-poetry.org/). Once cloned and within the project directory, install dependencies with:

```bash
poetry install
```

## Configuration

Requirements to get started are API Credentials in the form of an Username, Password, Client ID and Client Secret.

These are stored in the `config.json` file.

```json
# This is where the enviroment variables will go.  Things like
# API Keys, Client Ids, Etc

{
  "USERNAME": "",
  "PASSWORD": "",
  "CLIENT_ID": "",
  "CLIENT_SECRET": ""
}
```
If these keys are missing the tap will fail at run time with a warning of missing required keys.

### Usage

```
python3 -m tap_ireckonu.__init__ -c config.json -s state.json --catalog catalog.json > example.json
```

The above command, when run from a virtual environment with the poetry packages installed, will run the tap.  The tap will cycle through the endpoints starting with a beginning date of 2020-10-30 until the current date.  In production, subsequent runs will resume from the last date ran.  In dev - this feature isn't setup due to state and bookmarking being handled by a SQL Database.

# Streams

Streams are iterative endpoint calls that loop through paginated endpoints and the records they return, converting them to JSON strings that match the schema of the data returned.  

## Incremental Streams
All streams from the Treez API are setup to be Incremental.  These are iterated based on the Update field for each table.  The update field is then bookmarked for the next stream. (If there is not starting bookmark the beginning sync date is 2020-10-30).  All nested tables only go one nest deep - for information beyond that, refer to the individual schema files.

### Bulk Company
- Primary key fields: `id`
- Replication strategy: INCREMENTAL
- Replication based on: Updated daily timestamp (not based on a current field yet)
- Nested Tables:  
    - Addresses:
        - Table Name: `addresses`
        - Key Field: `id`
    - PhoneNumbers:
        - Table Name: `phonenumbers`
        - Key Field: `id`
    - External Resources:
        - Table Name: `externalresources`
    - Attributes:
        - Table Name: `attributes`
        - Key Field: `id`
- Transformations: none

### Bulk Person
- Primary key fields: `id`
- Replication strategy: INCREMENTAL
- Replication based on: Updated daily timestamp (not based on a current field yet)
- Nested Tables:
    - Names:
        - Table Name: `names`
        - Key Field: `id`
    - Addresses:
        - Table Name: `addresses`
        - Key Field: `id`
    - PhoneNumbers:
        - Table Name: `phonenumbers`
        - Key Field: `id`
    - Email Addresses:
        - Table Name: `emailaddresses`
        - Key Field: `id`
    - Company
        - Table Name: (See [Company](#bulk-company))
    - External Resources:
        - Table Name: `externalresources`
    - Attributes:
        - Table Name: `attributes`
        - Key Field: `id`
    - Data Consents:
        - Table Name: `dataconsents`
        - Key Field: `id`
    - Payment Methods:
        - Table Name: `paymentmethods`
        - Key Field: `id`
    - Preferences:
        - Table Name: `preferences`
        - Key Field: `id`
    - Loyalty Levels:
        - Table Name: `loyaltylevels`
        - Key Field: `id`
    - Stay Summaries:
        - Table Name: `staysummaries`
        - Key Field: `id`
    - Documents:
        - Table Name: `documents`
        - Key Field: `id`
    - Global Stays:
        - Table Name: `globalstays`
- Transformations: none

### Bulk Folios
- Primary key fields: `id`
- Replication strategy: INCREMENTAL
- Replication based on: Updated daily timestamp (not based on a current field yet)
- Nested Tables:
    - Addresses:
        - Table Name: `addresses`
        - Key Field: `id`
    - Lines:
        - Table Name: `lines`
        - Key Field: `invoicelinecode`
- Transformations: none

### Bulk Reservations
- Primary key fields: `id`
- Replication strategy: INCREMENTAL
- Replication based on: Updated daily timestamp (not based on a current field yet)
- Nested Tables:
    - Booker:
        - Table Name: `booker`
        - Based on: [Person](#bulk-person)
    - Primary Guest:
        - Table Name: `primaryguest`
        - Based on: [Person](#bulk-person)
    - Rate Plan:
        Table Name: `rateplan`
    - Agency:
        - Table Name: `agency`
        - Based on: [Company](#bulk-company)
    - Company:
        - Table Name: (See [Company](#bulk-company))`
    - Source Company
        - Table Name: `sourcecompany`
        - Based on: [Company](#bulk-company)
    - Extra Services
        - Table Name: `extraservices`
        - Key Field: `id`
    - Preferences:
        - Table Name: `preferences`
        - Key Field: `id`
    - External Resources:
        - Table Name: `externalresources`
    - Other Linked Persons:
        - Table Name: `otherlinkedpersons`
        - Based on: [Person](#bulk-person)
    - Contacts:
        - Table Name: `contacts`
        - Based on: [Person](#bulk-person)
    - Payment Informations:
        - Table Name: `paymentinformations`
    - Payment Methods:
        - Table Name: `paymentmethods`
        - Key Field: `id`
    - Attributes:
        - Table Name: `attributes`
        - Key Field: `id`
- Transformations: none

### Bulk House Accounts
