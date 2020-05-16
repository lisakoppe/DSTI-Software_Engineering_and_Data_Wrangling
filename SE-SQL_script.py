# ------------------------------------------------------------------------------------------------------------------------------
# Instruction:
# 6. The Python application should not require the user to install packages before the run.
# ------------------------------------------------------------------------------------------------------------------------------

def pkgs_install():
    """
    This function checks whether the required packages are already installed in the user's environment or not.
    If not, it automatically imports them without having the user doing any action before the run.
    """
    import subprocess
    import sys
    import importlib
    import os
    from os import path
    # Create a dictionary with the names and corresponding short names of the libraries/packages to be installed
    pkgs = {"pandas": "pd", "pyodbc": "pyodbc", "numpy": "np"}
    print(
        " ___________________________________________________\n|                                                   |\n|               Packages installation               |\n|___________________________________________________|\n")
    # Loop over the "pkgs" dictionary to check the packages installation
    for p in pkgs:
        # Initialize the variable "s" as the short name of the package
        s = pkgs[p]
        # Try to import the package "p"
        try:
            s = importlib.import_module(p)
        # If an error is returned, install the package with the pip manager
        except ImportError:
            print("[PKG] {} is not installed and has to be installed.".format(p))
            subprocess.call([sys.executable, "-m", "pip", "install", p])
        # Once installed, import the package
        finally:
            s = importlib.import_module(p)
            print('[PKG] {} is properly installed.'.format(p))
    print("\n[INFO] All packages have been imported. You're good to go!\n")


# ------------------------------------------------------------------------------------------------------------------------------
# Instruction:
# 1. Gracefully handle the connection to the database server.
# ------------------------------------------------------------------------------------------------------------------------------

def db_connection(driver, server, database):
    """
    This function takes as inputs the name of the SQL driver, the name of the user's server and the name of the database.
    It connects the user to the required database and returns the "sql_cnxn" variable allowing the connection.
    """
    # Make the "sql_cnxn" variable global so that it is accessible from everywhere (even outside this function)
    global sql_cnxn
    print(
        " ___________________________________________________\n|                                                   |\n|         Connection to the database server         |\n|___________________________________________________|\n")
    # Ensure the integration of the driver, the server and the database names to the connection string
    connection_string = "DRIVER={" + str(driver) + "}; " \
                        + "SERVER=" + str(server) + "; " \
                        + "DATABASE=" + str(database) + "; " \
                        + "Trusted_Connection=yes"
    print("[INFO] My complete connection command is: \n{}\n".format(connection_string))
    # Try to connect to the database with the connection string
    try:
        # Use pyodbc to connect to the database with the connection string
        sql_cnxn = pyodbc.connect(connection_string)
        # Test the connection with a query to display the "[dbo].[vw_AllSurveyData]" view
        my_query = "SELECT * FROM [" + str(database) + "].[dbo].[vw_AllSurveyData] ORDER BY SurveyId, UserId"
        df = pd.read_sql(my_query, sql_cnxn)
        # Visually check the structure of the database returned
        # The goal of this exercise is to get the same survey data after running the script
        print("[PREV] Preview of the 10 first rows of the database:\n\n", df.head(10), "\n")
        print("[CONN] The connection to the database server is established.\n")
        return sql_cnxn
    # If an error occurs, exit the program and return the name of the error
    except pyodbc.Error as err:
        sqlstate = err.args[1]
        raise SystemExit("[WARNING] Connection failed.\nPlease check error message:" + sqlstate)  # or use sys.exit()


def close_conn(conn_name):
    """
    This function takes as input the variable name of the connection previously established,
    closes and deletes the connection to the database server.
    """
    conn_name.close()
    del conn_name
    print("[CONN] The connection to the server has properly been closed and deleted.\n\n")


# ------------------------------------------------------------------------------------------------------------------------------
# Instruction:
# 2. Replicate the algorithm of the dbo.fn_GetAllSurveyDataSQL stored function.
# ------------------------------------------------------------------------------------------------------------------------------

def cursor_query(currentSurveyId):
    """
    This function takes as input the current survey id and include it as a string into the "currentQuestionCursor" query.
    It returns the complete query string in the "query_currentQuestionCursor" variable.
    """
    query_currentQuestionCursor = "SELECT * FROM (SELECT SurveyId, QuestionId, 1 as InSurvey FROM SurveyStructure " \
                                  + "WHERE SurveyId =" + str(currentSurveyId) + " UNION SELECT " + str(
        currentSurveyId) + " as SurveyId, " \
                                  + "Q.QuestionId, 0 as InSurvey FROM Question as Q WHERE NOT EXISTS(SELECT * FROM SurveyStructure as S " \
                                  + "WHERE S.SurveyId = " + str(
        currentSurveyId) + " AND S.QuestionId = Q.QuestionId)) as t " \
                                  + "ORDER BY QuestionId"
    return query_currentQuestionCursor


def get_db_struct(current_sql_cnxn):
    """
    This function takes as input the current connection variable outputted by the "db_connection()" function.
    It creates the main connection cursor variable and fetch on all surveys to build and output the structure of the survey as a dataframe.
    """
    # Create and open the main cursor
    surveyCursor1 = current_sql_cnxn.cursor()
    # Order the surveys by survey IDs
    surveyCursor1.execute("SELECT SurveyId FROM Survey ORDER BY SurveyId")
    # Initialization of the output data: multiple rows/lists to be stacked along 3 columns
    FRes = [0, 0, 0]
    # Loop over the survey ids
    for currentSurveyId in surveyCursor1.fetchall():
        # Initialize the "surveyId" variable as being the first element of the list "currentSurveyId"
        surveyId = currentSurveyId[0]
        # Create and open a second cursor
        surveyCursor2 = current_sql_cnxn.cursor()
        # Execute the "query_currentQuestionCursor" query with the survey ids
        surveyCursor2.execute(cursor_query(surveyId))
        # For each survey id, loop over "currentSurveyIdInQuestion", "currentQuestionId" and "currentInSurvey" to get the corresponding data
        for currentSurveyIdInQuestion, currentQuestionId, currentInSurvey in surveyCursor2.fetchall():
            # Create a row/list of data by survey id
            IRes = np.array([currentSurveyIdInQuestion, currentQuestionId, currentInSurvey])
            # Stack all the previously obtained rows/lists all together to get a matrix (list of lists)
            FRes = np.vstack((FRes, IRes))
    # Store the structure of the survey data "FRes" in a dataframe, without the first column of ids (with respect to the template)
    db_struct = pd.DataFrame(FRes[1:, :])
    # Add a header (names for each column of stacked data) and return the structure of the database "db_struct"
    db_struct.columns = ["SurveyId", "QuestionId", "QuestionInSurvey"]
    return db_struct
    # Close the two cursors previously opened
    surveyCursor1.close()
    surveyCursor2.close()


def set_strColumnsQueryPart(currentQuestionId, currentInSurvey):
    """
    This function takes as inputs the current question id and the current in survey outputted from the "db_struct" dataframe
    and sets two versions of a query depending on the input variables:
    - one type of query if the current question is in the current survey
    - one type of query if the current question is not in the current survey
    It creates the nested variables which will be looped over in the final query function.
    It returns the proper SQL string as "strColumnsQueryPart".
    """
    # Set the query string variables to get the correct values
    strQueryTemplateForAnswerColumn = "COALESCE((SELECT a.Answer_Value FROM Answer as a WHERE a.UserId = u.UserId " \
                                      + "AND a.SurveyId = <SURVEY_ID> AND a.QuestionId = <QUESTION_ID>), -1) " \
                                      + "AS ANS_Q<QUESTION_ID>"
    strQueryTemplateForNullColumn = "NULL AS ANS_Q<QUESTION_ID>"
    # If the current question is not in the current survey, the values in this column are set to NULL
    if currentInSurvey == 0:
        strColumnsQueryPart = strQueryTemplateForNullColumn.replace("<QUESTION_ID>", str(currentQuestionId))
    # If the current question is in the current survey, get the answer for the right survey id and question id
    else:
        strColumnsQueryPart = strQueryTemplateForAnswerColumn.replace("<QUESTION_ID>", str(currentQuestionId))
    # Return the "strColumnsQueryPart" variable as a string to make sure it will work in the next function
    return str(strColumnsQueryPart)


def set_strCurrentUnionQueryBlock(currentSurveyId, strColumnsQueryPart):
    """
    This function takes as inputs the current survey id and the string query created in the "set_strColumnsQueryPart()" function.
    It unions query pieces to output the "strCurrentUnionQueryBlock" string which will be used later to look over surveys.
    """
    # Define the string variable "strQueryTemplateOuterUnionQuery" and build the "strCurrentUnionQueryBlock" string with replacements
    strQueryTemplateOuterUnionQuery = "SELECT UserId, <SURVEY_ID> as SurveyId, <DYNAMIC_QUESTION_ANSWERS> " \
                                      + "FROM [User] as u WHERE EXISTS (SELECT * FROM Answer as a " \
                                      + "WHERE u.UserId = a.UserId AND a.SurveyId = <SURVEY_ID>)"
    strCurrentUnionQueryBlock = ""
    strCurrentUnionQueryBlock = strQueryTemplateOuterUnionQuery.replace("<DYNAMIC_QUESTION_ANSWERS>",
                                                                        str(strColumnsQueryPart))
    strCurrentUnionQueryBlock = strCurrentUnionQueryBlock.replace("<SURVEY_ID>", str(currentSurveyId))
    return str(strCurrentUnionQueryBlock)


def write_query(query_to_write):
    """
    This function takes an SQL query as input and save it to a text file.
    """
    # Make the "filepath" variable global so that it is accessible from everywhere
    global filepath
    # If the "outputs" folder does not exist yet, create it. If it does, do nothing
    if not path.exists("./outputs"):
        os.mkdir("./outputs")
    # Save the "query_to_write" string in a text file
    # n.b. the "with open" statement will automatically close the file once we're done
    filepath = "./outputs/saved_query.txt"
    with open(filepath, "w") as f:
        f.write(query_to_write)
        print("[PREV] Preview of the final query:\n\n'", query_to_write, "'\n")
        print("[SAVE] The final query has been saved to disk as saved_query.txt in the outputs folder.\n")


def read_query(query_to_read):
    """
    This function takes as input an SQL query, reads it and store it to another variable.
    """
    # Make the "my_final_query" variable global so that it is accessible from everywhere
    global my_final_query
    # Read the query and store it to the "my_final_query" variable
    with open(query_to_read, "r") as f:
        my_final_query = f.read()
        return my_final_query


def set_FinalQuery(survey_structure_df):
    """
    This function takes as input the survey structure dataframe created with the "get_db_struct()" function.
    It uses the previously defined functions to loop over the structure of the survey and then outputs the final query.
    It returns the final query saved in the "saved_query.txt" file.
    """
    # Initialize the variables to be used in the function
    # Create two lists of unique IDs for the questions and the surveys thanks to the returned variable of the function "get_db_struct()"
    list_QID = survey_structure_df["QuestionId"].unique()
    list_SID = survey_structure_df["SurveyId"].unique()
    # Create two variables which stores the value of the maximum idea in order to avoid placing a comma after the last items
    maxQID = np.max(list_QID)
    maxSID = np.max(list_SID)
    # Initialize the "strFinalQuery" variable as an empty string
    strFinalQuery = ""
    # Open the main loop to iterate over all the surveys thanks to their ids
    for survey_id in list_SID:
        # Initialize the "strIntermQuery" variable as an intermediate empty string
        strIntermQuery = ""
        # Open the inner loop to iterate over all the questions thanks to their ids
        for question_id in list_QID:
            # Integrate the loop variables "survey_id" and "question_id" into a new dataframe created from the "survey_structure_df"
            sub_df = survey_structure_df[
                (survey_structure_df["SurveyId"] == survey_id) & (survey_structure_df["QuestionId"] == question_id)]
            # Use ".iloc" indexing method to select rows and columns by number in the dataframe "survey_structure_df":
            # Select the last column "QuestionInSurvey" from the survey structure dataframe previously updated and return all the values (survey and questions ids) associated
            currentInSurvey_id = sub_df.iloc[:, -1].values
            # Update the "strIntermQuery" by adding the pieces of queries created from the "set_strColumnsQueryPart()" function
            strIntermQuery += set_strColumnsQueryPart(question_id, currentInSurvey_id)
            # Place a comma between column statements along question ids, except for the last one
            if question_id < maxQID:
                strIntermQuery += ", "
        strFinalQuery += set_strCurrentUnionQueryBlock(survey_id, strIntermQuery)
        # Place a "UNION" statement between column statements along survey ids, except for the last one
        if survey_id < maxSID:
            strFinalQuery += " UNION "
    # Save the freshly concatenated final query in a text file as "saved_query.txt"
    write_query(strFinalQuery)


# ------------------------------------------------------------------------------------------------------------------------------
# Instructions:
# 3. Replicate the algorithm of the trigger dbo.trg_refreshSurveyView for creating/altering the view vw_AllSurveyData
#    whenever applicable.
# 4. For achieving (3) above, a persistence component (in any format you like: CSV, XML, JSON, etc.), storing the last known
#    surveys’ structures should be in place.
# 5. Of course, extract the "always-fresh" pivoted survey data, in a CSV file, adequately named.
# ------------------------------------------------------------------------------------------------------------------------------

def check_view(sql_conn):
    """
    This function takes as input parameter "sql_conn", the connection string to access the database.
    It checks whether the survey view already exists in the current directory and, if so, if it has changed (inner if statement)
    If the file "updated_survey_view.csv" doesn't exist, it creates it and returns it.
    It returns the "my_final_query" file.
    """
    print(
        " ___________________________________________________\n|                                                   |\n|     Check the structure of the survey database    |\n|___________________________________________________|\n")
    # Initialize the "new_view" variable which is used in the inner if statement
    new_view = get_db_struct(sql_conn)
    # Check if the "updated_survey_view.csv" file already exists
    if path.exists("./outputs/updated_survey_structure.csv"):
        print("[INFO] The survey structure file has already been created.\n[INFO] Checking if the file has changed...")
        # Create the "current_view" variable which reads the .csv file
        current_view = pd.read_csv("./outputs/updated_survey_structure.csv", sep=",", index_col=0)
        # If the .csv file has not changed, do nothing
        if new_view.equals(current_view):
            print(
                "[INFO] The survey structure file has not changed.\n[INFO] It remains in the outputs folder as updated_survey_structure.csv.\n")
        # If the .csv file has changed, build the final query with the "set_FinalQuery()" function and save it to the same file
        else:
            print(
                "[INFO] The survey structure file has changed.\n[INFO] Updating the previous version of the file...\n")
            set_FinalQuery(new_view)
            new_view.to_csv("./outputs/updated_survey_structure.csv", sep=",")
            print("[PREV] Preview of the survey structure:\n\n", new_view, "\n")
            print("[SAVE] The survey structure file has successfully been updated as updated_survey_structure.csv.\n")
    # If the "updated_survey_view.csv" does not exist yet, build the final query with the "set_FinalQuery()" function and save it
    else:
        print("[INFO] The survey structure file doesn't exist yet.\n[INFO] Creating the survey structure file...")
        set_FinalQuery(new_view)
        new_view.to_csv("./outputs/updated_survey_structure.csv", sep=",")
        print("[PREV] Preview of the survey structure:\n\n", new_view, "\n")
        print(
            "[SAVE] The survey structure file has successfully been created in the outputs folder as updated_survey_structure.csv.")
        # Run the "read_query()" function to read the final query and store it to the "my_final_query" variable
    read_query(filepath)
    return my_final_query


# Define the "main()" function which gathers all the previously created functions organized in the correct order of execution to output the required result
def main(sql_driver, my_server, my_database):
    """
    This function takes as inputs the name of the SQL driver, the name of the user's server and the name of the database.
    It runs all the nested sub-functions and outputs the required result in a .csv file "AllSurveyDataSQL.csv" in the outputs folder.
    """
    # 0. Make the sql_cnxn variable global so that it is accessible from everywhere
    global sql_cnxn
    # 1. Make sure all the necessary packages are installed in the working environment
    pkgs_install()
    # 2. Connect to the database
    current_cnxn = db_connection(sql_driver, my_server, my_database)
    # 3. Check if the survey structure already exists and store the final query in the "my_final_query" variable
    my_final_query = check_view(current_cnxn)
    # 4. Read the final sql query to get all the survey data and save it as "AllSurveyDataSQL.csv"
    print(
        " ___________________________________________________\n|                                                   |\n|          Get and save all the survey data         |\n|___________________________________________________|\n")
    # 5. Create a dataframe from the final query previously saved
    df = pd.read_sql(my_final_query, current_cnxn)
    # 6. Save the dataframe as a .csv file
    df.to_csv("./outputs/AllSurveyDataSQL.csv")
    print("[PREV] Preview of the 10 first rows of the database:\n\n", df.head(10), "\n")
    print("[PREV] Preview of the 10 last rows of the database:\n\n", df.tail(10), "\n")
    print("[SAVE] Data have successfully been saved to the outputs folder as AllSurveyDataSQL.csv.\n")
    # 7. Close and delete the connection to the database server
    close_conn(sql_cnxn)
    print("// END SCRIPT //")


# Execute only if run as a script
if __name__ == "__main__":
    print("// BEGIN SCRIPT //\n")
    # Import the necessary libraries -> to be improved
    import os
    from os import path
    import numpy as np
    import pandas as pd
    import pyodbc

    # List all the pyodbc drivers and automatically select the right one: the driver should look like "ODBC Driver 17 for SQL Server"
    myDriver = [x for x in pyodbc.drivers() if x.endswith(" for SQL Server")]
    # Remove the "['']" around the name of the driver and store the name as a string
    myDriver = str(myDriver)[2:-2]
    # Ask the user to enter the name of the server to be used
    myServer = input("Please enter the server name you want to connect to: ")
    # Ask the user to enter the name of the databse to be used
    myDatabase = input("Please enter the name of the database you want to access to: ")
    print(
        " ___________________________________________________\n|                                                   |\n|    Initialization of the connection parameters    |\n|___________________________________________________|\n")
    print("[INFO] The SQL driver used is: " + str(myDriver))
    print("[INFO] The server used is: " + str(myServer))
    print("[INFO] The database used is: " + str(myDatabase) + "\n")
    # Run the main function
    main(myDriver, myServer, myDatabase)