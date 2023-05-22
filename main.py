import customtkinter
import InterfaceCreation
import os
import pandas as pd
import xlsxwriter

interface = InterfaceCreation.InterfaceCreation(customtkinter.CTk, 800, 650)
date = interface.get_date()

try:
    report_file_name = "InventoryAccuracyReport_{}.xlsx".format(date)

    path = os.path.join(os.path.expanduser("~"), "Downloads/{}".format(report_file_name))

    if os.path.exists(path):
        filename, extension = os.path.splitext(path)
        counter = 1
        while os.path.exists(path):
            path = filename + " (" + str(counter) + ")" + extension
            counter += 1

    str(path)

    global writer
    writer = pd.ExcelWriter(path, engine='xlsxwriter')

    workbook = writer.book
    worksheet1 = workbook.add_worksheet('Overview')
    number_format = workbook.add_format({'num_format': '0'})
    worksheet1.set_column('A:A', None, number_format)


    inventory_accuracy_report_sheet_name = "Overview"
    interface.set_report()
    interface.get_report().to_excel(writer, inventory_accuracy_report_sheet_name, startrow=0, startcol=0, index=False)

    writer.save()

    print("Inventory Accuracy Report Exported.")

except Exception as e:
    print(":: ERROR :: Could not export report!")
    print(e)
