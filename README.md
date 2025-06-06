#  SAS Customer Intelligence 360 Download Client: Python

## Overview
This Python script enables you to download cloud-hosted data tables from SAS Customer Intelligence 360.
 
The script can perform the following tasks:
 * Download the following data marts: `detail`, `dbtReport`, and `snapshot (previously identity)`.
 * Specify a time range to be downloaded.
 * Automatically unzip the download packages and create csv files with header rows and field delimiters.
 * Keep track of all initiated downloads. This lets you download a delta from the last complete download and append it to one file per table.

This topic contains the following sections:
* [Prerequisites](#prerequisites)
* [Using the Download Script](#using-the-download-script)
    * [Considerations](#considerations)
    * [Running the script](#running-the-script)
    * [Examples](#examples)
* [Contributing](#contributing)
* [License](#license)
* [Additional Resources](#additional-resources)



## Prerequisites
1. Install Python (version 3 or later) from https://www.python.org/.

   **Tip:** Select the option to add Python to your PATH variable. If you choose the advanced installation option, make sure to install the pip utility.
   
2. Make sure the following modules are installed for Python: `argparse`, `backoff`, `base64`, `codecs`, `csv`, `gzip`, `json`, `os`, 
`pandas` (version 1.3.0 or later), `PyJWT`, `requests`, `sys`, `time`, and `tqdm`.

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
4. Download the Python script from this repository and save it to your local machine.

5. In the `./dsccnfg/config.txt` file, set the following variables for your tenant:
   ```
     agentName = ci360_agent
     tenantId  = abc123-ci360-tenant-id-xyz
     secret    = ABC123ci360clientSecretXYZ
     baseUrl   = https://extapigwservice-<server>/marketingGateway/discoverService/dataDownload/eventData/
   ```

6. Verify the installation by running the following command from command prompt:  
```py discover.py –h```


## Using the Download Script

### Considerations
Before starting a download, make a note of the following things:
* When you use the option to create a CSV, choose a delimiter that is not present in the source data. You can also use the parameters to quote CSV fields to help you delineate columns.
* If data resets were processed and you download data in append mode, the old data is not deleted.  
  The new reset data for the same time period will be appended to the file.
* If you download data using schema 1 and then use append mode to download data using schema 6, the data is appended based on schema 6. However, the header rows in the existing file will not be updated.

### Running the Script

1. Open a command prompt.
2. Run the discover.py script with <a href="#script-parameters">parameter values</a> that are based on the tables that you want to download. For example, to download the detail tables with a start and end date range, you can run the following command:

   ```cmd
   py discover.py -m detail -st 2019-12-01T00 -et 2019-12-01T12
   ```

   ---
   **Note:** On Unix-like environments and Macs, the default `py` or `python` command might default to Python 2 if that version is    installed. Uninstall earlier versions of Python, or explicitly call Python 3 when you run script like this example:

   ```cmd
   python3 discover.py -m detail -st 2019-12-01T00 -et 2019-12-01T12
   ```

   You can verify which version runs by default with the following command: `python --version`

   ---

<a name="script-parameters"> </a>

These are the parameters to use when you run the discover.py script:

| Parameter &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; | Description       |
| :----------------------| :-----------------|
| -h, --help             | Displays the help |
| -m, --mart             | The table set to download. Use one of these values:<br><ul><li>detail (This value downloads Detail mart tables and the partitioned CDM tables - cdm_contact_history and cdm_response_history.)</li><li>dbtReport</li><li>snapshot (for CDM tables that are not partitioned, identity tables, and metadata tables)</li></ul>  |
| -svn,<br>--schemaversion  | Specify a specific schema of tables to download. For example: `17`. |
| -st, --start           | The start value in this datetime format: `yyyy-mm-ddThh` |
| -et, --end             | The end value in this datetime format: `yyyy-mm-ddThh`   |
| -ct, <br>--category        | The category of tables to download. When the parameter is not specified, you download tables for all the categories that you have a license to access.<br><br>To download tables from a specific category, you can use one of these values:<ul><li>cdm</li><li>discover</li><li>engagedigital</li><li>engagedirect</li><li>engagemetadata</li><li>engagemobile</li><li>engageweb</li><li>engageemail</li><li>optoutdata</li><li>plan</li></ul><br>For more information, see [Schemas and Categories](https://go.documentation.sas.com/?cdcId=cintcdc&cdcVersion=production.a&docsetId=cintag&docsetTarget=dat-export-api-sch.htm).| 
| -d, --delta            | Download only the changes (the delta) from the previous download. Set the value to `yes` or `no`. |
| -l, --limit            | For partitioned tables, specify a limit of partitions to download. For example, `-l 150` or `--limit=150` downloads only the first 150 partitions of a specific set.|
| -a, --append           | Append the download to the existing files. Set the value to `yes` or `no`. <br>The first time that you use the append option, a new file is created for each table without the datetimestamp prefix. Subsequent runs append records to these files. <br><br><hr>**Note:** For consistency, do not modify the CSV options for delimiter or quotation enclosure after the initial download (or after the first download that uses the append option).<br><br>During the first download with the append option enabled,  all fields in the combined file use the delimiter and quote options that are specified during that download call. However, for subsequent append operations the records use the delimiter and quote options for that specific download.<br><br>For example, if the original records used the delimiter "\|" but you enable append and set the delimiter to ",", all records in the combined file use the new delimiter. If you then change the delimiter to ";" only the newly appended records in the combined file use the ";" delimiter.<hr><br> |
| -cf, --csvflag         | Create a CSV file from the download tables. Set the value to `yes` or `no`.<br><br>**Note:** This parameter must be enabled for the other CSV parameters to take effect.|
| -cd, <br>--csvdelimiter    | For a CSV file, specify a delimiter other than the default pipe character ( `\|` ). |
| -ch, --csvheader       | For a CSV file, include a column header in the first row. Set the value to `yes` or `no`. |
| -cq, --csvquote        | For a CSV file, enclose field values in quotation marks ("). Set the value to `yes` or `no`.|
| -cqc, <br>--csvquotechar   | For a CSV file, override the default character (") that encloses field values. For example: `@`. |
| -cl, --clean           | Clean the download .zip files. By default, the files are deleted, but you can set this parameter to `no` to keep them. |

**Note:** The start and end ranges are only used for the script's first run. After the first run, the download history is stored in the dsccnfg directory. To force the script to use the variables for start date and end date, delete or move the history information.

   In addition, the values in the dataRangeStartTimeStamp column and dataRangeEndTimeStamp column in the download history tables are in the UTC time zone. The values in the download_dttm column are in the local time zone.

### Examples

* Download the Detail tables:
  
  ```py discover.py –m detail```
  
  Or:
  
  ```py discover.py –-mart=detail```

* Download the complete set of the CDM tables (both partitioned tables and non-partitioned tables):
  
  ```py discover.py –m snapshot -ct cdm```
  
  ```py discover.py –m detail -ct cdm```
  
  Or:
  
  ```py discover.py --mart=snapshot --category=cdm```  
  
  ```py discover.py --mart=detail --category=cdm```

* Download the detail tables (with only the delta from the last download), create a CSV file, and append to the existing files:
  
  ```py discover.py –m detail –d yes –cf yes –a yes```
  
  Or:
  
  ```py discover.py –-mart=detail –-delta=yes –-csvflag=yes –-append=yes```

* Download the detail tables for the specific time range from start hour (`-st`) to end hour (`-et`):
  
  ```py discover.py -m detail -st 2025-04-01T00 -et 2025-04-01T12```

  Or:

  ```py discover.py --mart=detail --start=2025-04-01T00 --end=2025-04-01T12```

* Download the discover base tables, create a CSV file, use the ";" (semicolon) delimiter, and include a column header in 
the first row:

  ```py discover.py -m dbtReport -cf yes -cd ";" -ch yes```

  Or:

  ```py discover.py --mart=dbtReport --csvflag=yes --csvdelimiter=";" --csvheader=yes```

* Download the detail tables with a specific schema (`-svn`), and specify a limit (`-l`) to download only the most recent 
150 partitions:
  
  ```py discover.py -m detail -svn 17 -l 150 -cf yes -cd "," -ch yes```

  Or:

  ```py discover.py --mart=detail --schemaversion=17 --limit=150 --csvflag=yes --csvdelimiter=";" --csvheader=yes```


## Contributing

We welcome your contributions! Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on how to submit contributions to this project.



## License

This project is licensed under the [Apache 2.0 License](LICENSE).



## Additional Resources
For more information, see [Downloading Data Tables with the REST API](https://go.documentation.sas.com/?softwareId=ONEMKTMID&softwareVersion=production.a&softwareContextId=DownloadDataTables) in the Help Center for SAS Customer Intelligence 360.
