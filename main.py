import psycopg2
import logging
import csv
import os
import numpy as np
import time
log = logging.getLogger(__name__)
logging.basicConfig(filename="logfile.txt",format="%(asctime)s --- %(message)s",level=logging.INFO)
log.info("Start")
def reconect(func):
    def wrapper():
        try:
            func()
        except psycopg2.errors.AdminShutdown:
            time.sleep(1)
            print("Введiть docker-compose up у термiнал. Потiм введiть:")
            wrapper()
    return wrapper()
@reconect
def main():
    #підключення до Вашої БД
    db1 = input("Please, print your db name")
    user = input("Please, print your username")
    password = input("Please, print your password")
    conn = psycopg2.connect("dbname='" + db1 +"' user='" + user + "' password='" + password + "'")
    print("Successful connection..")
    cursor = conn.cursor()
    #створення таблиці
    #колонки таблиці взяті з csv файлів, було додано колонку Year
    #Колонкам OUTID і Year видано Primary Key для уникнення дублікатів записів
    int_index = [1, 19, 20, 21, 30, 31, 40, 41, 50, 51, 60, 61, 70, 71, 80, 81, 89, 91, 99, 101, 109, 111, 119, 121]
    float_index=[18, 29, 39, 49, 59, 69, 79, 88, 98, 108, 118]
    lastrow = "CREATE TABLE IF NOT EXISTS LASTROW( number int);"
    cursor.execute(lastrow)
    conn.commit()
    cursor.execute("select count(*) from lastrow")
    counter = list(cursor.fetchone())[0]


    if counter == 0:

        inserting = "INSERT INTO LASTROW(number) VALUES(0);"
        cursor.execute(inserting)
        conn.commit()
    cursor.execute("select * from lastrow")
    last_row = list(cursor.fetchone())[0]
    if last_row !=0:

        print("До падіння було закомічено",last_row,"рядків, продовжуємо роботу")
    create = """CREATE TABLE IF NOT EXISTS RECONNECTTEST(
        OUTID varchar(100),
        Birth int,
        SEXTYPENAME varchar(10),
        REGNAME varchar(100),
        AREANAME varchar(100),
        TERNAME varchar(100),
        REGTYPENAME varchar(500),
        TerTypeName varchar(500),
        ClassProfileNAME varchar(500),
        ClassLangName varchar(500),
        EONAME varchar(400),
        EOTYPENAME varchar(100),
        EORegName varchar(100),
        EOAreaName varchar(100),
        EOTerName varchar(400),
        EOParent varchar(500),
        UkrTest varchar(500),
        UkrTestStatus varchar(100),
        UkrBall100 float ,
        UkrBall12 int ,
        UkrBall int ,
        UkrAdaptScale int ,
        UkrPTName varchar(500),
        UkrPTRegname varchar(500),
        UkrPTAreaName varchar(500),
        UkrPTTerName varchar(500),
        histTest varchar(500),
        HistLang varchar(500),
        histTestStatus varchar(100),
        histBall100 float,
        histBall12 int,
        histBall int,
        histPTName varchar(500),
        histPTRegname varchar(500),
        histPTAreaName varchar(500),
        histPTTerName varchar(500),
        mathTest varchar(500),
        mathLang varchar(20),
        mathTestStatus varchar(100),
        mathBall100 float ,
        mathBall12 int ,
        mathBall int ,
        mathPTName varchar(500),
        mathPTRegname varchar(500),
        mathPTAreaName varchar(500),
        mathPTTerName varchar(500),
        physTest varchar(500),
        physLang varchar(20),
        physTestStatus varchar(100),
        physBall100 float ,
        physBall12 int ,
        physBall int ,
        physPTName varchar(500),
        physPTRegname varchar(500),
        physPTAreaName varchar(500),
        physPTTerName varchar(500),
        chemTest varchar(500),
        chemLang varchar(20),
        chemTestStatus varchar(100),
        chemBall100 float ,
        chemBall12 int ,
        chemBall int ,
        chemPTName varchar(500),
        chemPTRegname varchar(500),
        chemPTAreaName varchar(500),
        chemPTTerName varchar(500),
        bioTest varchar(500),
        bioLang varchar(20),
        bioTestStatus varchar(100),
        bioBall100 float ,
        bioBall12 int ,
        bioBall int ,
        bioPTName varchar(500),
        bioPTRegname varchar(500),
        bioPTAreaName varchar(500),
        bioPTTerName varchar(500),
        geoTest varchar(500),
        geoLang varchar(20),
        geoTestStatus varchar(100),
        geoBall100 float ,
        geoBall12 int ,
        geoBall int ,
        geoPTName varchar(500),
        geoPTRegname varchar(500),
        geoPTAreaName varchar(500),
        geoPTTerName varchar(500),
        engTest varchar(500),
        engTestStatus varchar(100),
        engBall100 float ,
        engBall12 int ,
        engDPALevel varchar(400),
        engBall int ,
        engPTName varchar(500),
        engPTRegname varchar(500),
        engPTAreaName varchar(500),
        engPTTerName varchar(500),
        fraTest varchar(500),
        fraTestStatus varchar(100),
        fraBall100 float ,
        fraBall12 int ,
        fraDPALevel varchar(400),
        fraBall int ,
        fraPTName varchar(500),
        fraPTRegname varchar(500),
        fraPTAreaName varchar(500),
        fraPTTerName varchar(500),
        deuTest varchar(500),
        deuTestStatus varchar(100),
        deuBall100 float ,
        deuBall12 int ,
        deuDPALevel varchar(400),
        deuBall int ,
        deuPTName varchar(500),
        deuPTRegname varchar(500),
        deuPTAreaName varchar(500),
        deuPTTerName varchar(500),
        spaTest varchar(500),
        spaTestStatus varchar(100),
        spaBall100 float,
        spaBall12 int ,
        spaDPALevel varchar(400),
        spaBall int ,
        spaPTName varchar(500),
        spaPTRegname varchar(500),
        spaPTAreaName varchar(500),
        spaPTTerName varchar(500),
        Year int,
        PRIMARY KEY(OUTID, Year));"""
    cursor.execute(create)
    conn.commit()
    with open('Odata2019File.csv') as csv_file:
        reader = csv.reader(csv_file, delimiter=';')
        columns = ','.join(next(reader))
        columns = columns+",Year"
        number = 0
        for lines in reader:
            if number <= last_row:
                number = number + 1
                continue
            m_old = lines
            m = []
            for j in m_old:
                m.append(j.strip('"').replace('\'','`'))
            for i in int_index:
                if m[i] == 'null':
                    m[i] = 0
                else:
                    m[i] = int(m[i])
            for j in float_index:
                if m[j] == 'null':
                    m[j] = 0.0
                else:
                    m[j] = float(m[j].replace(',','.'))

            query = f"INSERT INTO RECONNECTTEST({columns}) VALUES ('" + str(m[0]) + "', '" + str(m[1]) + "','" + str(m[2]) + "','" + str(m[3]) + "','" + str(m[4]) + "','" + str(m[5]) + "','" + str(m[6]) + "','" + str(m[7]) + "','" + str(m[8]) + "','" + str(m[9]) + "','" + str(m[10]) + "','" + str(m[11]) + "','" + str(m[12]) + "','" + str(m[13]) + "','" + str(m[14]) + "','" + str(m[15]) + "','" + str(m[16]) + "','" + str(m[17]) + "','" + str(m[18]) + "','" + str(m[19]) + "','" + str(m[20]) + "','" + str(m[21]) + "','" + str(m[22]) + "','" + str(m[23]) + "','" + str(m[24]) + "','" + str(m[25]) + "','" + str(m[26]) + "','" + str(m[27]) + "','" + str(m[28]) + "','" + str(m[29]) + "','" + str(m[30]) + "','" + str(m[31]) + "','" + str(m[32]) + "','" + str(m[33]) + "','" + str(m[34]) + "','" + str(m[35]) + "','" + str(m[36]) + "','" + str(m[37]) + "','" + str(m[38]) + "','" + str(m[39]) + "','" + str(m[40]) + "','" + str(m[41]) + "','" + str(m[42]) + "','" + str(m[43]) + "','" + str(m[44]) + "','" + str(m[45]) + "','" + str(m[46]) + "','" + str(m[47]) + "','" + str(m[48]) + "','" + str(m[49]) + "','" + str(m[50]) + "','" + str(m[51]) + "','" + str(m[52]) + "','" + str(m[53]) + "','" + str(m[54]) + "','" + str(m[55]) + "','" + str(m[56]) + "','" + str(m[57]) + "','" + str(m[58]) + "','" + str(m[59]) + "','" + str(m[60]) + "','" + str(m[61]) + "','" + str(m[62]) + "','" + str(m[63]) + "','" + str(m[64]) + "','" + str(m[65]) + "','" + str(m[66]) + "','" + str(m[67]) + "','" + str(m[68]) + "','" + str(m[69]) + "','" + str(m[70]) + "','" + str(m[71]) + "','" + str(m[72]) + "','" + str(m[73]) + "','" + str(m[74]) + "','" + str(m[75]) + "','" + str(m[76]) + "','" + str(m[77]) + "','" + str(m[78]) + "','" + str(m[79]) + "','" + str(m[80]) + "','" + str(m[81]) + "','" + str(m[82]) + "','" + str(m[83]) + "','" + str(m[84]) + "','" + str(m[85]) + "','" + str(m[86]) + "','" + str(m[87]) + "','" + str(m[88]) + "','" + str(m[89]) + "','" + str(m[90]) + "','" + str(m[91]) + "','" + str(m[92]) + "','" + str(m[93]) + "','" + str(m[94]) + "','" + str(m[95]) + "','" + str(m[96]) + "','" + str(m[97]) + "','" + str(m[98]) + "','" + str(m[99]) + "','" + str(m[100]) + "','" + str(m[101]) + "','" + str(m[102]) + "','" + str(m[103]) + "','" + str(m[104]) + "','" + str(m[105]) + "','" + str(m[106]) + "','" + str(m[107]) + "','" + str(m[108]) + "','" + str(m[109]) + "','" + str(m[110]) + "','" + str(m[111]) + "','" + str(m[112]) + "','" + str(m[113]) + "','" + str(m[114]) + "','" + str(m[115]) + "','" + str(m[116]) + "','" + str(m[117]) + "','" + str(m[118]) + "','" + str(m[119]) + "','" + str(m[120]) + "','" + str(m[121]) + "','" + str(m[122]) + "','" + str(m[123]) + "','" + str(m[124]) + "','" + str(m[125]) + "',2019)"
            try:

                cursor.execute(query)
            except Exception as exception:
                log.info("Breaking database. Please reconnect")

                raise exception

            if number % 100 == 0:
                try:
                    cursor.execute(f"UPDATE lastrow SET number={number}")
                    conn.commit()
                    log.info("another 100 rows added")
                    conn.commit()
                except Exception as exception:

                    log.info("Breaking database. Please reconnect")

                    raise exception
            number = number + 1
        try:
            conn.commit()
        except Exception as exception:
            log.info("Breaking database. Please reconnect")

            raise exception


    with open('Odata2020File.csv') as csv_file:
        reader = csv.reader(csv_file, delimiter=';')
        columns = ','.join(next(reader))
        columns = columns+",Year"
        number = 0
        for lines in reader:
            number = number + 1
            m_old = lines
            m = []
            for j in m_old:
                m.append(j.strip('"').replace('\'','`'))

            for i in int_index:
                if m[i] == 'null':
                    m[i] = 0
                else:
                    m[i] = int(m[i])
            for j in float_index:
                if m[j] == 'null':
                    m[j] = 0.0
                else:
                    m[j] = float(m[j].replace(',','.'))

            query = f"INSERT INTO RECONNECTTEST({columns}) VALUES ('" + str(m[0]) + "', '" + str(m[1]) + "','" + str(m[2]) + "','" + str(m[3]) + "','" + str(m[4]) + "','" + str(m[5]) + "','" + str(m[6]) + "','" + str(m[7]) + "','" + str(m[8]) + "','" + str(m[9]) + "','" + str(m[10]) + "','" + str(m[11]) + "','" + str(m[12]) + "','" + str(m[13]) + "','" + str(m[14]) + "','" + str(m[15]) + "','" + str(m[16]) + "','" + str(m[17]) + "','" + str(m[18]) + "','" + str(m[19]) + "','" + str(m[20]) + "','" + str(m[21]) + "','" + str(m[22]) + "','" + str(m[23]) + "','" + str(m[24]) + "','" + str(m[25]) + "','" + str(m[26]) + "','" + str(m[27]) + "','" + str(m[28]) + "','" + str(m[29]) + "','" + str(m[30]) + "','" + str(m[31]) + "','" + str(m[32]) + "','" + str(m[33]) + "','" + str(m[34]) + "','" + str(m[35]) + "','" + str(m[36]) + "','" + str(m[37]) + "','" + str(m[38]) + "','" + str(m[39]) + "','" + str(m[40]) + "','" + str(m[41]) + "','" + str(m[42]) + "','" + str(m[43]) + "','" + str(m[44]) + "','" + str(m[45]) + "','" + str(m[46]) + "','" + str(m[47]) + "','" + str(m[48]) + "','" + str(m[49]) + "','" + str(m[50]) + "','" + str(m[51]) + "','" + str(m[52]) + "','" + str(m[53]) + "','" + str(m[54]) + "','" + str(m[55]) + "','" + str(m[56]) + "','" + str(m[57]) + "','" + str(m[58]) + "','" + str(m[59]) + "','" + str(m[60]) + "','" + str(m[61]) + "','" + str(m[62]) + "','" + str(m[63]) + "','" + str(m[64]) + "','" + str(m[65]) + "','" + str(m[66]) + "','" + str(m[67]) + "','" + str(m[68]) + "','" + str(m[69]) + "','" + str(m[70]) + "','" + str(m[71]) + "','" + str(m[72]) + "','" + str(m[73]) + "','" + str(m[74]) + "','" + str(m[75]) + "','" + str(m[76]) + "','" + str(m[77]) + "','" + str(m[78]) + "','" + str(m[79]) + "','" + str(m[80]) + "','" + str(m[81]) + "','" + str(m[82]) + "','" + str(m[83]) + "','" + str(m[84]) + "','" + str(m[85]) + "','" + str(m[86]) + "','" + str(m[87]) + "','" + str(m[88]) + "','" + str(m[89]) + "','" + str(m[90]) + "','" + str(m[91]) + "','" + str(m[92]) + "','" + str(m[93]) + "','" + str(m[94]) + "','" + str(m[95]) + "','" + str(m[96]) + "','" + str(m[97]) + "','" + str(m[98]) + "','" + str(m[99]) + "','" + str(m[100]) + "','" + str(m[101]) + "','" + str(m[102]) + "','" + str(m[103]) + "','" + str(m[104]) + "','" + str(m[105]) + "','" + str(m[106]) + "','" + str(m[107]) + "','" + str(m[108]) + "','" + str(m[109]) + "','" + str(m[110]) + "','" + str(m[111]) + "','" + str(m[112]) + "','" + str(m[113]) + "','" + str(m[114]) + "','" + str(m[115]) + "','" + str(m[116]) + "','" + str(m[117]) + "','" + str(m[118]) + "','" + str(m[119]) + "','" + str(m[120]) + "','" + str(m[121]) + "','" + str(m[122]) + "','" + str(m[123]) + "','" + str(m[124]) + "','" + str(m[125]) + "',2020)"
            try:

                cursor.execute(query)
            except Exception as exception:
                log.info("Breaking database. Please reconnect")

                raise exception

            if number % 100 == 0:
                try:
                    cursor.execute(f"UPDATE lastrow SET number={number}")
                    conn.commit()
                    log.info("another 100 rows added")
                    conn.commit()
                except Exception as exception:

                    log.info("Breaking database. Please reconnect")

                    raise exception
            number = number + 1
        try:
            conn.commit()
        except Exception as exception:
            log.info("Breaking database. Please reconnect")

            raise exception
    print("Варіант 11. Порівняти середній бал з фізики в 2019 2020 для кожного регіону")

    query = """SELECT Year, AVG(physBall100), physPTRegName FROM RECONNECTTEST
                   WHERE physTestStatus='Зараховано'
                   GROUP BY physPTRegName, Year
                   ORDER BY Year;
                   """
    cursor.execute(query)
    phys = np.array(cursor.fetchall())
    region = set(phys[:, 2])
    add_to_file = {}
    for i in region:
        mean = np.where(phys == i)[0]
        add_to_file[i] = [phys[mean[0]][1], phys[mean[1]][1]]

    if os.path.exists('result.csv'):
        os.remove('result.csv')
    with open('result.csv','w', newline='') as csv_file:
        writer = csv.writer(csv_file, delimiter=',')
        writer.writerow(["region", "mean2019", "mean2020"])
        for i in region:
            writer.writerow([i, add_to_file[i][0], add_to_file[i][1]])


if __name__ == '__main__':
    try:
        main()
    except TypeError:
        pass
