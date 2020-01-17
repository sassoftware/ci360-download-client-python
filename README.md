#  SAS Customer Intelligence 360 Download Client: Python

## Overview
Download the client program to download SAS Customer Intelligence 360 data from cloud.
  The script can perform the following tasks:
 * Download the data marts: detail, dbtReport, identity.
 * Specify a time range to be downloaded.
 * Automatically unzip and create csv files with header lines and individual delimiters.
 * Keep track of all initiated downloads to be able to download the complete delta to the last download and also append it into one file per table.


### What's New

### Prerequisites
* Python3
 make sure following python libraries are installed:
 `requests`, `base64`, `json`, `PyJWT`, `gzip`, `csv`, `codecs`, `os`, `sys`, `argparse`, `time`, `tqdm`, `pandas` and `backoff`.
* from CI360 Console - General Settings -> External -> Access 
 get the following ( create new access point if it's not already created)
```
 External gateway address: e.g. https://extapigwservice-dev.cidev.sas.us/marketingGateway
 Name: ci360_agent
 Tenant ID: abc123-ci360-tenant-id-xyz
 Client secret: ABC123ci360clientSecretXYZ
```
### Installation
* Download client program and save it to client machine.
* Set up python3 with required libraries.
* Set the following variables for your tenant in ../dsccnfg/config.txt file:
```
  agentName = ci360_agent
  tenantId  = abc123-ci360-tenant-id-xyz
  secret    = ABC123ci360clientSecretXYZ
  baseUrl   = https://extapigwservice-dev.cidev.sas.us/marketingGateway/discoverService/dataDownload/eventData/
```

* Verfify installation by running the following command from command prompt:
```
  discover.py –h
```
## Getting Started
Before downloading make a note of the following things:
* While creating CSV choose the delimiter which will not be present in data.
* In case of resets, if the user is downloading the data in an append mode the old data will not be deleted.  
The new reset data for the same range will be appended to the file.
* If user has downloaded data using schema1 and now downloading using schema3 in append mode, the data will be appended as per schema 3 but the old headers will not be updated.

### Running

* Open command prompt.
* Depending on which mart to download submit the command. For example for detail mart with start and end date range you can run the following command:
```
  discover.py -m detail -st 2019-12-01T00 -et 2019-12-01T12
```

### Examples
```
# Get help
discover.py –h

# Download detail data mart
discover.py –m detail

# Download dbtReport data mart
discover.py –m dbtReport

# Download the identity tables
discover.py –m identity

# Download detail data mart, only the delta to the last download, 
# create a CSV file and append
discover.py –m detail –d yes –cf yes –a yes	

# Download detail data mart, only for the specific time range
# from start hour to end hour
discover.py -m detail -st 2019-12-01T00 -et 2019-12-01T12

# Download dbtReport data mart, create CSV file and # use delimiter “;” (semicolon), 
# include a column header line in the first row
discover.py -m dbtReport -cf yes -cd ";" -ch yes

# Same as before, but the option "-cl no" will keep the downloaded zip files
# in the download folder.
discover.py -m dbtReport -cf yes -cd ";" -ch yes -cl no
```

### Troubleshooting


## Contributing

We welcome your contributions! Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on how to submit contributions to this project.

## License

This project is licensed under the [Apache 2.0 License](LICENSE).

## Additional Resources
For more information, see [Downloading Data from SAS Customer Intelligence 360](https://go.documentation.sas.com/?cdcId=cintcdc&cdcVersion=production.a&docsetId=cintag&docsetTarget=extapi-discover-service.htm&locale=en#p0kj5ymn5wuyqdn1209mw0xcfinc).

