import mysql.connector
import csv
from datetime import datetime


cnx = mysql.connector.connect(user='corp',
                              password='corp',
                              host='172.27.2.8',
                              database='bitrix_corp_analytics')

time_start = datetime.now()
with cnx.cursor() as cursor:
    result = cursor.execute("""
    SELECT b.name as ticket,
        b.date_create,
        b.timestamp_x,
        biep.PROPERTY_135 as subproejct,
        biep.PROPERTY_77 as time,
        biep.PROPERTY_79 as user_id
    FROM b_iblock_element as b
    LEFT JOIN b_iblock_element_prop_s24 biep ON b.ID = biep.IBLOCK_ELEMENT_ID
    WHERE b.IBLOCK_ID=24 and b.DATE_CREATE >= '2022-09-01 00:00:00';
    """)
    rows = cursor.fetchall()
    with open("tracking_time.csv", mode="w", encoding='utf-8') as w_file:
        file_writer = csv.writer(w_file, delimiter=",", lineterminator="\r")
        file_writer.writerow(["id", "Дата создания", "Дата списания", "ID подпроекта", "Кол-во списанных часов", "ID пользователя", "Месяц списания", "Подпроект|Месяц"])
        for row in rows:
            if all(element is not None for element in row):
                # print(str(row[0]) + " | "
                #       + "{:%d.%m.%Y}".format(row[1]) + " | "
                #       + "{:%d.%m.%Y}".format(row[2]) + " | "
                #       + str(int(row[3])) + " | "
                #       + str("{:.2f}".format(row[4] / 60).replace('.', ',')) + " | "
                #       + str(int(row[5])) + " | "
                #       + str(row[2].replace(day=1).day) + "." + str(row[2].replace(day=1).month) + "." + str(row[2].replace(day=1).year) + " | "
                #       + str(int(row[3]))+"|"+str(row[2].replace(day=1).day) + "." + str(row[2].replace(day=1).month) + "." + str(row[2].replace(day=1).year))

                file_writer.writerow([
                    str(row[0]),
                    "{:%d.%m.%Y}".format(row[1]),
                    "{:%d.%m.%Y}".format(row[2]),
                    str(int(row[3])),
                    "{:.2f}".format(row[4] / 60).replace('.', ','),
                    str(int(row[5])),
                    str(row[2].replace(day=1).day) + "." + str(row[2].replace(day=1).month) + "." + str(row[2].replace(day=1).year),
                    str(int(row[3])) + "|" + str(row[2].replace(day=1).day) + "." + str(row[2].replace(day=1).month) + "." + str(row[2].replace(day=1).year),
                ])

cnx.close()
time_end = datetime.now()
print("Время выполнения скрипта (сек): " + str((time_end - time_start).total_seconds()))
