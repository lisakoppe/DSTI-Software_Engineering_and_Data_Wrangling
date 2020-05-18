# DSTI - Software Engineering and Data Wrangling with SQL

## 01. Requirements

Create a Python 3 application which:

1. Gracefully handle the connection to the database server.

2. Replicate the algorithm of the dbo.fn_GetAllSurveyDataSQL stored function.

3. Replicate the algorithm of the trigger dbo.trg_refreshSurveyView for creating/altering the view vw_AllSurveyData whenever applicable.

4. For achieving (3) above, a persistence component (in any format you like: CSV, XML, JSON, etc.), storing the last known surveys’ structures should be in place.

5. Of course, extract the "always-fresh" pivoted survey data, in a CSV file, adequately named.

6. The Python application should not require the user to install packages before the run.


## 02. Resources

All resources used are available in the folder ["Resources"](https://github.com/lisakoppe/DSTI-Software_Engineering_and_Data_Wrangling/tree/master/Resources).

The script uses the [pyodbc](https://github.com/mkleehammer/pyodbc) package to connect to the SQL database.


## 03. Application architecture

![mapping_fn_and_var](https://github.com/lisakoppe/DSTI-Software_Engineering_and_Data_Wrangling/blob/master/mapping_fn_and_var.png)

The Python application is available [here](https://github.com/lisakoppe/DSTI-Software_Engineering_and_Data_Wrangling/blob/master/SE-SQL_script.py).


## 04. Outputs

![expected_script_results](https://github.com/lisakoppe/DSTI-Software_Engineering_and_Data_Wrangling/blob/master/expected_script_results.png)

All outputed files are gathered in the folder ["Outputs"](https://github.com/lisakoppe/DSTI-Software_Engineering_and_Data_Wrangling/tree/master/Outputs).
