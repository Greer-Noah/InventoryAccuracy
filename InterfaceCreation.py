from tkinter import *
from tkinter import filedialog
import customtkinter
import os
import mysql.connector
import pandas as pd
from pandas.io import sql as sql
from pyxlsb import open_workbook as open_xlsb
from pyepc import SGTIN
from pyepc.exceptions import DecodingError
from sqlalchemy import create_engine
import pymysql
import openpyxl


global date

global cycle_count_paths
global transactions_path
global item_file_path
global golden_skus_path

global report

global app
global conn
global cursor


def import_cycle_count():
    print("Importing Cycle Count...")
    pop_up_title = "Select Cycle Count Data (.xlsx)"
    filenames = filedialog.askopenfilenames(initialdir="/", title=pop_up_title,
                                            filetypes=(("txt files", "*.txt"), ("all files", "*.*")))
    global cycle_count_paths
    cycle_count_paths = []
    for filename in filenames:
        cycle_count_paths.append(filename)
    print(cycle_count_paths)


def decodePreparation():
    epc_list = []
    for filename in cycle_count_paths:
        f = open(filename, "r")
        lines = f.readlines()
        for x in lines:
            epc_list.append(x.split('\n')[0])
        f.close()

    epc_list_no_dupe = [*set(epc_list)]
    epc_list_df = pd.DataFrame(epc_list_no_dupe, columns=['EPCs'])

    print("Preparing to Decode...")
    return epc_list_df


def decodeCycleCount(epc_list_df):
    epc_list = []
    columns = epc_list_df.columns.tolist()

    for _, i in epc_list_df.iterrows():
        for col in columns:
            epc_list.append(i[col])

    temp_epc_list = []
    for epc in epc_list:
        temp_epc_list.append(str(epc))

    epc_list = temp_epc_list

    res = list(map(''.join, epc_list))
    epc_list = [*set(res)]

    upc_list, error_epcs, error_upcs = [], [], []

    print("Decoding...")
    for epc in epc_list:
        try:
            upc_list.append(SGTIN.decode(epc).gtin)
        except DecodingError as de:
            error_epcs.append(epc)
            error_upcs.append(de)
        except TypeError as te:
            error_epcs.append(epc)
            error_upcs.append(te)

    for epc in error_epcs:
        if epc in epc_list:
            epc_list.remove(epc)

    for upc in range(len(upc_list)):
        upc_list[upc] = upc_list[upc].lstrip('0')

    return epc_list, upc_list


def import_golden_skus():
    print("Golden SKUs...")
    pop_up_title = "Select Golden SKUs File (.xlsx)"
    filename = filedialog.askopenfilename(initialdir="/", title=pop_up_title,
                                          filetypes=(("xlsx files", "*.xlsx"), ("all files", "*.*")))
    global golden_skus_path
    golden_skus_path = filename
    print(golden_skus_path)


def import_item_file():
    print("Item File...")
    pop_up_title = "Select Item File (GM) (.csv)"
    filename = filedialog.askopenfilename(initialdir="/", title=pop_up_title,
                                          filetypes=(("csv files", "*.csv"), ("all files", "*.*")))
    global item_file_path
    item_file_path = filename
    print(item_file_path)


def import_transactions_file():
    print("Transactions File...")
    pop_up_title = "Select Transactions File (.csv)"
    filename = filedialog.askopenfilename(initialdir="/", title=pop_up_title,
                                          filetypes=(("csv files", "*.csv"), ("all files", "*.*")))
    global transactions_path
    transactions_path = filename
    print(transactions_path)


def validate_date_input():
    global date
    date = date_entry.get()
    try:
        if date == "":
            return False
        date_list = date.split(".")
        if len(date_list[0]) == 4 and isinstance(int(date_list[0]), int):
            if len(date_list[1]) == 2 and isinstance(int(date_list[1]), int):
                if len(date_list[2]) == 2 and isinstance(int(date_list[2]), int):
                    print("Date: {}".format(date))

                    return True
    except:
        print(":: ERROR :: Date input is not valid!")
        return False


def validate_inputs():
    valid_inputs = True
    try:
        if validate_date_input() is False:
            valid_inputs = False
    except Exception as e:
        print(":: ERROR :: Incorrect Date Input!")
        print(e)
        return False

    try:
        if cycle_count_paths == "":
            valid_inputs = False
    except Exception as e:
        print(":: ERROR :: Cycle Count Data file paths have not been specified!")
        print(e)
        return False

    try:
        if item_file_path == "":
            valid_inputs = False
    except Exception as e:
        print(":: ERROR :: Item File path has not been specified!")
        print(e)
        return False

    try:
        if golden_skus_path == "":
            valid_inputs = False
    except Exception as e:
        print(":: ERROR :: Golden SKUs path has not been specified!")
        print(e)
        return False

    try:
        if transactions_path == "":
            valid_inputs = False
    except Exception as e:
        print(":: ERROR :: Transactions path has not been specified!")
        print(e)
        return False

    return valid_inputs


def import_cycle_count_sql(epc_list, upc_list):
    print("Creating UPC Drop Table...")
    stmt = "DROP TABLE if exists UPCDrop;"
    cursor.execute(stmt)
    stmt1 = "CREATE TABLE if not exists UPCDrop(EPCs text, UPCs bigint);"
    cursor.execute(stmt1)
    cursor.executemany("""
                INSERT INTO UPCDrop(EPCs, UPCs)
                VALUES (%s, %s)
            """, list(zip(epc_list, upc_list)))

    conn.commit()
    print('Data entered successfully.')


def import_golden_skus_sql():
    print("Creating Golden SKUs Table...")
    stmt = "DROP TABLE if exists GoldenSKUs;"
    cursor.execute(stmt)
    stmt1 = "CREATE TABLE if not exists GoldenSKUs(UPCs bigint);"
    cursor.execute(stmt1)
    golden_skus_df = pd.read_excel(golden_skus_path, sheet_name=0)
    golden_skus_list = golden_skus_df['UPCs'].tolist()
    cursor.executemany("""
                    INSERT INTO GoldenSKUs(UPCs)
                    VALUES (%s)
                """, list(zip(golden_skus_list)))

    conn.commit()
    print('Data entered successfully.')


def import_item_file_sql():
    stmt = "DROP TABLE IF EXISTS ItemFile"
    cursor.execute(stmt)
    statement_headers = "CREATE TABLE ItemFile(store_number int, REPL_GROUP_NBR int, gtin bigint, ei_onhand_qty int, " \
                        "SNAPSHOT_DATE text, UPC_NBR bigint, UPC_DESC text, ITEM1_DESC text, dept_nbr int, " \
                        "DEPT_DESC text, MDSE_SEGMENT_DESC text, MDSE_SUBGROUP_DESC text, ACCTG_DEPT_DESC text, " \
                        "DEPT_CATG_GRP_DESC text, DEPT_CATEGORY_DESC text, DEPT_SUBCATG_DESC text, VENDOR_NBR int, " \
                        "VENDOR_NAME text, BRAND_OWNER_NAME text, BRAND_FAMILY_NAME text)"
    cursor.execute(statement_headers)

    # --------------Loads both Item Files into single ItemFile table------------------------------------------------
    item_file_path_corrected = item_file_path.replace(" ", "\\ ")

    stmt = "LOAD DATA LOCAL INFILE \'{}\' " \
           "INTO TABLE ItemFile " \
           "CHARACTER SET latin1 " \
           "FIELDS TERMINATED BY \',\' " \
           "OPTIONALLY ENCLOSED BY \'\"\' " \
           "LINES TERMINATED BY \'\\r\\n\' " \
           "IGNORE 1 ROWS;".format(item_file_path_corrected)

    print(" -- Starting Item File import...")
    cursor.execute(stmt)
    print(" -- Item File import complete.")
    conn.commit()
    cursor.execute("ALTER TABLE ItemFile ADD COLUMN UPC_No_Check bigint AFTER gtin;")
    cursor.execute("UPDATE ItemFile SET UPC_No_Check = LEFT(gtin, length(gtin)-1);")
    conn.commit()


def import_transactions_sql():
    try:
        stmt = "DROP TABLE IF EXISTS TransactionsData"
        cursor.execute(stmt)
        statement_headers = "CREATE TABLE TransactionsData(date_hour text, Event_Timestamp bigint, STORE_NBR int, " \
                            "dept_nbr int, CID bigint, eGTIN bigint, Transaction_Type text, Transaction_QTY int, " \
                            "InventoryState text, Transaction_QTY2 int, Aggregate_Qty int)"
        cursor.execute(statement_headers)
        transactions_path_corrected = transactions_path.replace(" ", "\\ ")

        stmt = "LOAD DATA LOCAL INFILE \'{}\' " \
               "INTO TABLE TransactionsData " \
               "CHARACTER SET latin1 " \
               "FIELDS TERMINATED BY \',\' " \
               "ENCLOSED BY \'\"\' " \
               "LINES TERMINATED BY \'\\r\\n\' " \
               "IGNORE 1 ROWS;".format(transactions_path_corrected)
        print(" -- Starting Transactions Data import...")
        cursor.execute(stmt)
        print(" -- Transactions Data import complete.")
        conn.commit()
        cursor.execute("ALTER TABLE TransactionsData ADD COLUMN UPC_No_Check bigint AFTER eGTIN;")
        cursor.execute("UPDATE TransactionsData SET UPC_No_Check = LEFT(eGtin, length(eGtin)-1);")
        conn.commit()
    except Exception as e:
        print(":: ERROR :: Could not import Transactions Data!")
        print(e)



def create_total_items_sql():
    try:
        print(" -- Creating Total Items...")
        cursor.execute("DROP TABLE IF EXISTS TotalItems;")
        stmt = """
                    CREATE TABLE TotalItems AS
                    SELECT DISTINCT UPCDrop.EPCs,
                    itemfile.gtin,
                    itemfile.DEPT_CATG_GRP_DESC,
                    itemfile.DEPT_CATEGORY_DESC, 
                    itemfile.VENDOR_NBR,
                    itemfile.VENDOR_NAME,
                    itemfile.BRAND_FAMILY_NAME,
                    itemfile.dept_nbr
                    FROM itemfile
                    INNER JOIN UPCDrop ON UPCDrop.UPCs = itemfile.gtin
                    WHERE UPCDrop.UPCs = itemfile.gtin and dept_nbr IN ('7','9','14','17','20','22','71','72','74','87');
                  """
        cursor.execute(stmt)
        cursor.execute("ALTER TABLE TotalItems ADD COLUMN UPC_No_Check bigint AFTER dept_nbr;")
        cursor.execute("UPDATE TotalItems SET UPC_No_Check = LEFT(gtin, length(gtin)-1);")
        conn.commit()
        print(" -- Total Items created.")
    except Exception as e:
        print(":: ERROR :: Could not create Total Items!")
        print(e)


def create_overview_sql():
    try:
        print(" -- Generating Inventory Accuracy Report Overview...")
        cursor.execute("DROP TABLE IF EXISTS inventoryaccuracyoverview;")
        stmt = """
            CREATE TABLE inventoryaccuracyoverview AS
            SELECT gs.UPCs AS UPC, COALESCE(ti.RFID, 0) AS RFID, COALESCE(if1.OH, 0) AS OH,
                CASE
                    WHEN COALESCE(if1.OH, 0) > COALESCE(ti.RFID, 0) THEN 'Overstated'
                    WHEN COALESCE(if1.OH, 0) < COALESCE(ti.RFID, 0) THEN 'Understated'
                    ELSE 'Match'
                END AS Status,
                COALESCE(ti.RFID, 0) - COALESCE(if1.OH, 0) AS Difference,
                COALESCE(td.Transaction_Qty, 0) AS 'Sale/Return',
                if2.dept_nbr AS Dept, if2.Vendor_NBR AS 'Vendor NBR', if2.Vendor_Name AS 'Vendor Name', if2.ITEM1_DESC AS 'Item Desc'
            FROM goldenskus gs
            LEFT JOIN (
                SELECT UPC_No_Check, COUNT(UPC_No_Check) AS RFID
                FROM TotalItems
                GROUP BY UPC_No_Check
            ) ti ON gs.UPCs = ti.UPC_No_Check
            LEFT JOIN (
                SELECT UPC_No_Check, SUM(ei_onhand_qty) AS OH
                FROM ItemFile
                GROUP BY UPC_No_Check
            ) if1 ON gs.UPCs = if1.UPC_No_Check
            LEFT JOIN (
                SELECT UPC_No_Check, SUM(transaction_qty) AS Transaction_Qty
                FROM TransactionsData
                GROUP BY UPC_No_Check
            ) td ON gs.UPCs = td.UPC_No_Check
            LEFT JOIN ItemFile if2 ON gs.UPCs = if2.UPC_No_Check;
        """
        cursor.execute(stmt)
        conn.commit()
        print(" -- Inventory Accuracy Report Overview Generated.")
    except Exception as e:
        print(":: ERROR :: Could not create Overview!")
        print(e)


def export_report():

    connection = mysql.connector.connect(user='root', password='password', host='127.0.0.1',
                                         database='InventoryAccuracy',
                                         allow_local_infile=True)

    report_overview_df = sql.read_sql('SELECT * FROM inventoryaccuracyoverview', connection)
    global report
    report = report_overview_df

    connection.close()


def submit_info():
    if validate_inputs():
        print("Successfully submitted. Starting Inventory Accuracy Report Generation...")
        connect_to_mysql()
        epc_list_df = decodePreparation()
        epc_list, upc_list = decodeCycleCount(epc_list_df)
        import_cycle_count_sql(epc_list, upc_list)
        import_golden_skus_sql()
        import_item_file_sql()
        import_transactions_sql()
        create_total_items_sql()
        create_overview_sql()
        export_report()
        print("Inventory Accuracy Report Generated. Press Quit to Export.")

    else:
        print("\n------------------------------------------------------------------------"
              "\n:: ERROR :: Invalid date input! Please enter a valid input before submitting!"
              "\n------------------------------------------------------------------------")


def connect_to_mysql():
    try:
        global conn
        conn = mysql.connector.connect(user='root', password='password', host='127.0.0.1', database='inventoryaccuracy',
                                       allow_local_infile=True)
        global cursor
        cursor = conn.cursor()
        stmt00 = "SET GLOBAL local_infile=1;"
        cursor.execute(stmt00)
        print("Connected to MySQL...")
    except:
        print(":: ERROR :: Something went wrong! Unable to connect to MySQL!")


def quit_app():
    print("Quit...")
    app.quit()


class InterfaceCreation:

    def __init__(self, root, w, h):
        self.root = root
        self.width = w
        self.height = h
        self.date_input = None
        self.report = None

    def get_date(self):
        return date_entry.get()

    def set_report(self):
        global report
        self.report = report

    def get_report(self):
        return self.report

    customtkinter.set_appearance_mode("Dark")
    customtkinter.set_default_color_theme("dark-blue")
    global app
    app = customtkinter.CTk()
    app.title("Inventory Accuracy Reporting System")
    app.geometry("800x600")


    '''
    Frame Creation
    '''
    global main_frame
    main_frame = customtkinter.CTkFrame(master=app, fg_color="transparent")
    middle_frame = customtkinter.CTkFrame(master=main_frame, fg_color="transparent")
    top_frame = customtkinter.CTkFrame(master=main_frame, fg_color="transparent")
    bottom_frame = customtkinter.CTkFrame(master=main_frame, fg_color="transparent")

    main_frame.pack(fill="both", expand=True)

    middle_frame.pack(anchor='center', fill="y", expand=True)
    bottom_frame.pack(side=BOTTOM, fill="x")

    '''
    Date Entry Creation
    '''

    global date_entry
    date_entry = customtkinter.CTkEntry(master=middle_frame, placeholder_text="Date (YYYY.MM.DD)")

    date_entry.pack(padx=30, pady=50)

    '''
    Button Creation
    '''
    cycle_count_button = customtkinter.CTkButton(master=middle_frame, text="Cycle Counts (.txt)", command=import_cycle_count)
    golden_skus_button = customtkinter.CTkButton(master=middle_frame, text="Golden SKUs (.xlsx)", command=import_golden_skus)
    item_file_button = customtkinter.CTkButton(master=middle_frame, text="Item File (.csv)", command=import_item_file)
    transactions_button = customtkinter.CTkButton(master=middle_frame, text="Transactions (.csv)", command=import_transactions_file)
    submit_button = customtkinter.CTkButton(master=middle_frame, text="Submit", command=submit_info)
    quit_button = customtkinter.CTkButton(master=middle_frame, text="Quit", command=quit_app)

    cycle_count_button.pack(pady=10)
    golden_skus_button.pack(pady=10)
    item_file_button.pack(pady=10)
    transactions_button.pack(pady=10)
    submit_button.pack(side=RIGHT, padx=10, pady=50)
    quit_button.pack(side=LEFT, padx=10)

    app.mainloop()