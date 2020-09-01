![](header.svg)

CHADBuilder (part of the 'In Data We Trust' project)
=======================================================

CHADBuilder is a database detailing the hospital events, pathology and radiology of patients 
admitted to Cardiff & Vale Health Board in Wales, UK during the COVID-19 outbreak. This is part
of the "In Data We Trust" project led by [Dr Mark Ponsford](https://www.cardiff.ac.uk/people/view/1245689-Ponsford-Mark) 
at Cardiff University. Data extraction was performed by Leitchan Smith (C&V IT) and [Ross Burton](https://www.linkedin.com/in/burtonbiomedical/?originalSubdomain=uk).
The CHADBuilder source code and SQLite database was created by Ross Burton.

CHADBuilder version 0.1 is an SQLite database, with anticipation to generate a NoSQL MongoDB instance 
in future releases once public data sharing has been approved. The SQLite schema can be found in schema.pdf

The code presented in this repository is licensed under the MIT open source license.

Usage
------

1. obtain data from securefileshare:

```python
from CHADBuilder.fetch_data import get_pages, get_files
pages = get_pages() # Fetches the page content
get_files(pages, output_dir="/home/user/Downloads/securefileshare_downloads") # Downloads files into target directory
```

2. consolidate files by category:

```python
from CHADBuilder.process_data import consolidate
consolidate(read_path="/home/user/Downloads/securefileshare_downloads",
            write_path="/home/user/Downloads/securefileshare_downloads/consolidated")
```

3. create database and populate using extracted files:

```python
from CHADBuilder.populate import Populate
# By initialising this object, the database is generated with a standard schema (see schema.pdf)
pop = Populate(database_path="/home/user/CHADBuilder.db",
               data_path="/home/user/Downloads/securefileshare_downloads/consolidated")
pop.populate() # Populate the tables of the database (takes 10 - 15 minutes)
pop.create_indexes() # Creates some useful indexes to optimise searches
pop.close()
```




