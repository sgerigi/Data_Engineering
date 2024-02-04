import pyodbc

cnxn_str = ('data source=sanjeeva\MSSQLSERVER01;initial catalog=museums;trusted_connection=true')
cnxn = pyodbc.connect(cnxn_str)