Architecture and building of an ETL pipeline which can extract APIs and local files. 

The file is as follow :

1. Loading functions
2. Data Profiling (infos, dtypes, missing values etc)
3. Transformation functions (mapping, replace, sorting, convert_dtypes)
4. Aggregate and exporting the dataset

I have then assembled this pipeline in order to clean raw sales data and Power BI Ready. The pipeline is modular which is easy to add/edit or remove functionallity. To use it you basically need Python and pandas and run the Pipeline_data.py.



Configuration
The project use a config.json file to handle :
1. Access paths (Source/Export).
2. Mapping
3. Aggregate conditions

Tech Stack:
Python 3.x
Pandas (Core engine)
Requests (API integration)
