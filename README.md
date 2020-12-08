#  SAS Customer Intelligence 360 Download Client: Python

## Overview
This Python script enables you to download cloud-hosted data tables from SAS Customer Intelligence 360.
 
The script can perform the following tasks:
 * Download the following data marts: `detail`, `dbtReport`, and `snapshot (previously identity)`.
 * Specify a time range to be downloaded.
 * Automatically unzip the download packages and create csv files with header rows and field delimiters.
 * Keep track of all initiated downloads. This lets you download a delta from the last complete download and append it to one file per table.

This topic contains the following sections:
* <a href="#prereq">Prerequisites</a>
* <a href="#install">Installation</a>
* <a href="#runscript">Running the Script</a> (includes examples)
* <a href="#contributing">Contributing</a>
* <a href="#license">License</a>
* <a href="#resources">Additional Resources</a>


<a id="prereq"> </a>

## Prerequisites
1. Install Python (version 3 or later) from https://www.python.org/.

   **Tip:** Select the option to add Python to your PATH variable. If you choose the advanced installation option, make sure to install the pip utility.
   
2. Make sure the following modules are installed for Python: `argparse`, `backoff`, `base64`, `codecs`, `csv`, `gzip`, `json`, `os`, 
`pandas`, `PyJWT`, `requests`, `sys`, `time`, and `tqdm`.

     In most cases, many of the modules are installed by default. To list all packages that are installed with Python 
     (through pip or by default), use this command:  
     ```python -c help('modules')```
     
     **Tip:** In most situations, you can install the non-default packages with this command:  
     ```pip install backoff pandas PyJWT requests tqdm```
  

3. Create an access point in SAS Customer Intelligence 360.
    1. From the user interface, navigate to **General Settings** > **External Access** > **Access Points**.
    2. Create a new access point if one does not exist.
    3. Get the following information from the access point:  
       ```
        External gateway address: e.g. https://extapigwservice-<server>/marketingGateway  
        Name: ci360_agent  
        Tenant ID: abc123-ci360-tenant-id-xyz  
        Client secret: ABC123ci360clientSecretXYZ  
       ```

<a id="install"> </a>

## Installation
1. Download the Python script and save it to your local machine.
2. Set up python3 with required libraries.
3. In the `./dsccnfg/config.txt` file, set the following variables for your tenant:
```
  agentName = ci360_agent
  tenantId  = abc123-ci360-tenant-id-xyz
  secret    = ABC123ci360clientSecretXYZ
  baseUrl   = https://extapigwservice-<server>/marketingGateway/discoverService/dataDownload/eventData/
```

4. Verify the installation by running the following command from command prompt:  
```py discover.py –h```


<a id="runscript"> </a>

## Running the Script

### Considerations
Before starting a download, make a note of the following things:
* When you use the option to create a CSV, choose a delimiter that is not present in the source data.
* If data resets were processed and you download data in append mode, the old data is not deleted.  
  The new reset data for the same time period will be appended to the file.
* If you download data using schema 1 and then use append mode to download data using schema 3, the data is appended based on schema 3. However, the header rows in the existing file will not be updated.

### Using the Script

1. Open a command prompt.
2. Depending on which mart to download submit the command. For example for detail mart with start and end date range you can run the following command:
```
  py discover.py -m detail -st 2019-12-01T00 -et 2019-12-01T12
```

### Examples

* See the help:  
```py discover.py –h```

* Download the detail tables:  
```py discover.py –m detail```

* Download the discover Base tables:  
```py discover.py –m dbtReport```

* Download the snapshot tables:  
```py discover.py –m snapshot```

* Download the detail tables (with only the delta from the last download), create a CSV file, and append to the existing files:  
```py discover.py –m detail –d yes –cf yes –a yes```

* Download the detail tables for the specific time range from start hour (`-st`) to end hour (`-et`):  
```py discover.py -m detail -st 2019-12-01T00 -et 2019-12-01T12```

* Download the discover base tables, create a CSV file, use the ";" (semicolon) delimiter, and include a column header in 
the first row:  
```py discover.py -m dbtReport -cf yes -cd ";" -ch yes```

* This example is similar to the previous example, but the option `-cl no` keeps the downloaded zip files in the download 
folder:  
```py discover.py -m dbtReport -cf yes -cd ";" -ch yes -cl no```

* Download the detail tables with a specific schema (`-svn`), and specify a limit (`-l`) to download only the most recent 
150 partitions:  
```py discover.py -m detail -svn 3 -l 150 -cf yes -cd "," -ch yes```

* Download the Plan data tables, create a CSV file, use the ";" (semicolon) delimiter, and include a column header in 
the first row:  
```py discover.py -m snapshot -ct plan -svn 5 -cf yes -cd ";" -ch yes```

<a id="contributing"> </a>

## Contributing

We welcome your contributions! Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on how to submit contributions to this project.


<a id="license"> </a>

## License

This project is licensed under the [Apache 2.0 License](LICENSE).


<a id="resources"> </a>

## Additional Resources
For more information, see [Downloading Data from SAS Customer Intelligence 360](https://go.documentation.sas.com/?cdcId=cintcdc&cdcVersion=production.a&docsetId=cintag&docsetTarget=extapi-discover-service.htm&locale=en#p0kj5ymn5wuyqdn1209mw0xcfinc).

