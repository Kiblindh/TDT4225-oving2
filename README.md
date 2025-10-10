## Get started

1. Clone repo with git clone "TDT4225-oving2"
2. Download porto dataset from blackboard. Be sure to add the porto dataset to the root of the oving2 folder
3. Run: pip install -r requirements.txt (This does not work for a python model newer than 3.12)
4. To connect to the database make sure to create a .env file in the root folder (oving2) with the following variables:

- DATABASE_HOST = "tdt4225-06.idi.ntnu.no"
- DATABASE_NAME = ""
- DATABASE_USER = ""
- DATABASE_PASSWORD = ""
  => The values for DATABASE_NAME, DATABASE_USER and DATABASE_PASSWORD is located in the report
  => If another host is utilized, then the DATABASE_HOST will need to be modified also

5. Make sure that you are connected to the NTNU network or using the NTNU VPN
6. IF YOU WANT TO REPOPULATE DATABASE: Run the "createtables.py" file

- Be careful since this takes around 5 to 6 hours
- Can skip this step if you want to run queries on the populated database

7. If you want to run queries:

- Run the "Queries.py" file
- The result of the queries will be located in the results folder
