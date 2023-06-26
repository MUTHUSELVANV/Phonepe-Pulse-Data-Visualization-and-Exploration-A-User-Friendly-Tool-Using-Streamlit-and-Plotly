# Phonepe-Pulse-Data-Visualization-and-Exploration-A-User-Friendly-Tool-Using-Streamlit-and-Plotly
The code is designed to extract data from the Phonepe pulse Github repository and provides several functionalities. It allows users to export the data to a MySQL database and perform operations on it, such as extracting or deleting records. Additionally, the code enables the visualization of the data through interactive plots.

The process begins by establishing a connection to the MySQL database using the provided credentials. It checks if the specified database exists and creates it if necessary. The code then retrieves the list of tables present in the database and displays them as options in a dropdown menu.

If there are tables available, the user can select a table to perform actions on. They can choose to drop a table, which deletes it from the database. The code updates the table list accordingly and provides a success message.

In case no tables exist, an error message is displayed, indicating that no data has been exported to MySQL. The connection to the database is closed.

For data analysis, the code retrieves data from the Phonepe pulse repository. It accesses specific folders and files containing transaction information. It organizes the data into lists and creates pandas DataFrames for further processing.

The code then enables the user to select a state and displays the corresponding data for analysis. It performs calculations and manipulations on the data, such as aggregating and filtering. The results are stored in variables.

To visualize the data, the code utilizes the seaborn library to create bar plots. It iterates over the data and generates plots for each selected year, comparing labels and amounts. The plots are displayed along with appropriate labels and titles.

Lastly, the code includes interactive buttons for executing the different functionalities, such as exporting data, dropping tables, and displaying plots.

Overall, the code provides a comprehensive solution for extracting, manipulating, and visualizing data from the Phonepe pulse Github repository using MySQL and interactive plots.
