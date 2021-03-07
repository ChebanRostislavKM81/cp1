import psycopg2
import logging
import csv
import os
import numpy as np
import time
log = logging.getLogger(__name__)
logging.basicConfig(filename="logfile.txt",format="%(asctime)s --- %(message)s",level=logging.INFO)
log.info("Початок роботи")
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
    create = """CREATE TABLE IF NOT EXISTS CHEBANKM81(
        OUTID varchar(100),
        Birth varchar(15),
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
        UkrBall100 varchar(10) NULL,
        UkrBall12 varchar(10) NULL,
        UkrBall varchar(10) NULL,
        UkrAdaptScale varchar(10) NULL,
        UkrPTName varchar(500),
        UkrPTRegname varchar(500),
        UkrPTAreaName varchar(500),
        UkrPTTerName varchar(500),
        histTest varchar(500),
        HistLang varchar(500),
        histTestStatus varchar(100),
        histBall100 varchar(10) NULL,
        histBall12 varchar(10) NULL,
        histBall varchar(10) NULL,
        histPTName varchar(500),
        histPTRegname varchar(500),
        histPTAreaName varchar(500),
        histPTTerName varchar(500),
        mathTest varchar(500),
        mathLang varchar(20),
        mathTestStatus varchar(100),
        mathBall100 varchar(10) NULL,
        mathBall12 varchar(10) NULL,
        mathBall varchar(10) NULL,
        mathPTName varchar(500),
        mathPTRegname varchar(500),
        mathPTAreaName varchar(500),
        mathPTTerName varchar(500),
        physTest varchar(500),
        physLang varchar(20),
        physTestStatus varchar(100),
        physBall100 varchar(10) NULL,
        physBall12 varchar(10) NULL,
        physBall varchar(10) NULL,
        physPTName varchar(500),
        physPTRegname varchar(500),
        physPTAreaName varchar(500),
        physPTTerName varchar(500),
        chemTest varchar(500),
        chemLang varchar(20),
        chemTestStatus varchar(100),
        chemBall100 varchar(10) NULL,
        chemBall12 varchar(10) NULL,
        chemBall varchar(10) NULL,
        chemPTName varchar(500),
        chemPTRegname varchar(500),
        chemPTAreaName varchar(500),
        chemPTTerName varchar(500),
        bioTest varchar(500),
        bioLang varchar(20),
        bioTestStatus varchar(100),
        bioBall100 varchar(10) NULL,
        bioBall12 varchar(10) NULL,
        bioBall varchar(10) NULL,
        bioPTName varchar(500),
        bioPTRegname varchar(500),
        bioPTAreaName varchar(500),
        bioPTTerName varchar(500),
        geoTest varchar(500),
        geoLang varchar(20),
        geoTestStatus varchar(100),
        geoBall100 varchar(10) NULL,
        geoBall12 varchar(10) NULL,
        geoBall varchar(10) NULL,
        geoPTName varchar(500),
        geoPTRegname varchar(500),
        geoPTAreaName varchar(500),
        geoPTTerName varchar(500),
        engTest varchar(500),
        engTestStatus varchar(100),
        engBall100 varchar(10) NULL,
        engBall12 varchar(10) NULL,
        engDPALevel varchar(400),
        engBall varchar(10) NULL,
        engPTName varchar(500),
        engPTRegname varchar(500),
        engPTAreaName varchar(500),
        engPTTerName varchar(500),
        fraTest varchar(500),
        fraTestStatus varchar(100),
        fraBall100 varchar(10) NULL,
        fraBall12 varchar(10) NULL,
        fraDPALevel varchar(400),
        fraBall varchar(10) NULL,
        fraPTName varchar(500),
        fraPTRegname varchar(500),
        fraPTAreaName varchar(500),
        fraPTTerName varchar(500),
        deuTest varchar(500),
        deuTestStatus varchar(100),
        deuBall100 varchar(10) NULL,
        deuBall12 varchar(10) NULL,
        deuDPALevel varchar(400),
        deuBall varchar(10) NULL,
        deuPTName varchar(500),
        deuPTRegname varchar(500),
        deuPTAreaName varchar(500),
        deuPTTerName varchar(500),
        spaTest varchar(500),
        spaTestStatus varchar(100),
        spaBall100 varchar(10),
        spaBall12 varchar(10) NULL,
        spaDPALevel varchar(400),
        spaBall varchar(10) NULL,
        spaPTName varchar(500),
        spaPTRegname varchar(500),
        spaPTAreaName varchar(500),
        spaPTTerName varchar(500),
        Year varchar(10) NULL,
        PRIMARY KEY(OUTID, Year));"""
    cursor.execute(create)
    conn.commit()
    cursor.execute("select count(*) from chebankm81")
    conn.commit()
    count = cursor.fetchone()
    if list((count))[0] == 0:
        #наповнення таблиці, якщо вона порожня
        a = open('Odata2019File.csv', 'r')
        lines = a.readlines()

        columns = ','.join(lines[0].replace('"','').split(';'))
        columns = columns+",Year"
        for i in range(1, len(lines)):
            m_old = lines[i]
            m_old = m_old.split(';')
            m_older = []
            for j in m_old:
                m_older.append(j.strip('"'))
                m=[]
                for jj in m_older:
                    jj_new = jj.replace('\'','`')
                    m.append(jj_new)

            query = f"INSERT INTO CHEBANKM81({columns}) VALUES ('" + m[0] + "', '" + m[1] + "','" + m[2] + "','" + m[3] + "','" + m[4] + "','" + m[5] + "','" + m[6] + "','" + m[7] + "','" + m[8] + "','" + m[9] + "','" + m[10] + "','" + m[11] + "','" + m[12] + "','" + m[13] + "','" + m[14] + "','" + m[15] + "','" + m[16] + "','" + m[17] + "','" + m[18] + "','" + m[19] + "','" + m[20] + "','" + m[21] + "','" + m[22] + "','" + m[23] + "','" + m[24] + "','" + m[25] + "','" + m[26] + "','" + m[27] + "','" + m[28] + "','" + m[29] + "','" + m[30] + "','" + m[31] + "','" + m[32] + "','" + m[33] + "','" + m[34] + "','" + m[35] + "','" + m[36] + "','" + m[37] + "','" + m[38] + "','" + m[39] + "','" + m[40] + "','" + m[41] + "','" + m[42] + "','" + m[43] + "','" + m[44] + "','" + m[45] + "','" + m[46] + "','" + m[47] + "','" + m[48] + "','" + m[49] + "','" + m[50] + "','" + m[51] + "','" + m[52] + "','" + m[53] + "','" + m[54] + "','" + m[55] + "','" + m[56] + "','" + m[57] + "','" + m[58] + "','" + m[59] + "','" + m[60] + "','" + m[61] + "','" + m[62] + "','" + m[63] + "','" + m[64] + "','" + m[65] + "','" + m[66] + "','" + m[67] + "','" + m[68] + "','" + m[69] + "','" + m[70] + "','" + m[71] + "','" + m[72] + "','" + m[73] + "','" + m[74] + "','" + m[75] + "','" + m[76] + "','" + m[77] + "','" + m[78] + "','" + m[79] + "','" + m[80] + "','" + m[81] + "','" + m[82] + "','" + m[83] + "','" + m[84] + "','" + m[85] + "','" + m[86] + "','" + m[87] + "','" + m[88] + "','" + m[89] + "','" + m[90] + "','" + m[91] + "','" + m[92] + "','" + m[93] + "','" + m[94] + "','" + m[95] + "','" + m[96] + "','" + m[97] + "','" + m[98] + "','" + m[99] + "','" + m[100] + "','" + m[101] + "','" + m[102] + "','" + m[103] + "','" + m[104] + "','" + m[105] + "','" + m[106] + "','" + m[107] + "','" + m[108] + "','" + m[109] + "','" + m[110] + "','" + m[111] + "','" + m[112] + "','" + m[113] + "','" + m[114] + "','" + m[115] + "','" + m[116] + "','" + m[117] + "','" + m[118] + "','" + m[119] + "','" + m[120] + "','" + m[121] + "','" + m[122] + "','" + m[123] + "','" + m[124] + "','" + m[125] + "',2019)"
            cursor.execute(query)
        conn.commit()
        a.close()
        a = open('Odata2020File.csv', 'r')
        lines = a.readlines()

        columns = ','.join(lines[0].replace('"','').split(';'))
        columns = columns+",Year"
        for i in range(1, len(lines)):
            m_old = lines[i]
            m_old = m_old.split(';')
            m_older = []
            for j in m_old:
                m_older.append(j.strip('"'))
                m=[]
                for jj in m_older:
                    jj_new = jj.replace('\'','`')
                    m.append(jj_new)
            query = f"INSERT INTO CHEBANKM81({columns}) VALUES ('" + m[0] + "', '" + m[1] + "','" + m[2] + "','" + m[3] + "','" + m[4] + "','" + m[5] + "','" + m[6] + "','" + m[7] + "','" + m[8] + "','" + m[9] + "','" + m[10] + "','" + m[11] + "','" + m[12] + "','" + m[13] + "','" + m[14] + "','" + m[15] + "','" + m[16] + "','" + m[17] + "','" + m[18] + "','" + m[19] + "','" + m[20] + "','" + m[21] + "','" + m[22] + "','" + m[23] + "','" + m[24] + "','" + m[25] + "','" + m[26] + "','" + m[27] + "','" + m[28] + "','" + m[29] + "','" + m[30] + "','" + m[31] + "','" + m[32] + "','" + m[33] + "','" + m[34] + "','" + m[35] + "','" + m[36] + "','" + m[37] + "','" + m[38] + "','" + m[39] + "','" + m[40] + "','" + m[41] + "','" + m[42] + "','" + m[43] + "','" + m[44] + "','" + m[45] + "','" + m[46] + "','" + m[47] + "','" + m[48] + "','" + m[49] + "','" + m[50] + "','" + m[51] + "','" + m[52] + "','" + m[53] + "','" + m[54] + "','" + m[55] + "','" + m[56] + "','" + m[57] + "','" + m[58] + "','" + m[59] + "','" + m[60] + "','" + m[61] + "','" + m[62] + "','" + m[63] + "','" + m[64] + "','" + m[65] + "','" + m[66] + "','" + m[67] + "','" + m[68] + "','" + m[69] + "','" + m[70] + "','" + m[71] + "','" + m[72] + "','" + m[73] + "','" + m[74] + "','" + m[75] + "','" + m[76] + "','" + m[77] + "','" + m[78] + "','" + m[79] + "','" + m[80] + "','" + m[81] + "','" + m[82] + "','" + m[83] + "','" + m[84] + "','" + m[85] + "','" + m[86] + "','" + m[87] + "','" + m[88] + "','" + m[89] + "','" + m[90] + "','" + m[91] + "','" + m[92] + "','" + m[93] + "','" + m[94] + "','" + m[95] + "','" + m[96] + "','" + m[97] + "','" + m[98] + "','" + m[99] + "','" + m[100] + "','" + m[101] + "','" + m[102] + "','" + m[103] + "','" + m[104] + "','" + m[105] + "','" + m[106] + "','" + m[107] + "','" + m[108] + "','" + m[109] + "','" + m[110] + "','" + m[111] + "','" + m[112] + "','" + m[113] + "','" + m[114] + "','" + m[115] + "','" + m[116] + "','" + m[117] + "','" + m[118] + "','" + m[119] + "','" + m[120] + "','" + m[121] + "','" + m[122] + "','" + m[123] + "','" + m[124] + "','" + m[125] + "',2020)"
            cursor.execute(query)
        conn.commit()
        a.close()
    #cтворення сsv файлу
    if os.path.exists("result.csv"):
        os.remove("result.csv")
    with open( "result.csv", 'a', newline='') as filedata:
        filewriter = csv.writer(filedata, delimiter=',')
        filewriter.writerow(["region", "mean2019", "mean2020"])
    #починаємо виконувати запит згідно варіанту
    print("Варіант 11. Порівняти середній бал з фізики за 2019 і 2020 роки в кожному регіоні")
    cursor.execute("select physPTRegName from chebankm81 where Year='2019' and physTestStatus='Зараховано'")
    conn.commit()
    hist = cursor.fetchall()
    cursor.execute("select physPTRegName from chebankm81 where Year='2020' and physTestStatus='Зараховано'")
    conn.commit()
    hist2020 = cursor.fetchall()
    a2019=(list(set((list((hist))))))
    a2020 = (list(set((list((hist2020))))))
    a = []
    for i in a2019:
        for j in a2020:
            if i == j:
                a.append(i)
                break
    def query(listq):
        if listq == []:
            log.info("Закінчення роботи")
            print("Не забудьте переглянути файли logfile.txt i result.csv, вони мали створитися")
            return 0
        else:
            print("Для регіону ",list((listq[0]))[0],":")
            cursor.execute("select physBall100 from chebankm81 where physPTRegName='" + list((listq[0]))[0] + "'  and Year='2019' and physTestStatus='Зараховано'")
            conn.commit()
            hist = cursor.fetchall()
            cursor.execute("select physBall100 from chebankm81 where physPTRegName='" + list((listq[0]))[0] + "'  and Year='2020' and physTestStatus='Зараховано'")
            conn.commit()
            hist2020 = cursor.fetchall()
            mean2019=[]
            mean2020=[]
            for i in range(len(hist)):
                mean2019.append(float((list(hist[i]))[0].replace(',','.')))
            mean2019 = np.array(mean2019)
            print("За 2019 рік:",np.mean(mean2019))
            for i in range(len(hist2020)):
                mean2020.append(float((list(hist2020[i]))[0].replace(',','.')))
            mean2020 = np.array(mean2020)
            print("За 2020 рік:",np.mean(mean2020))

            with open("result.csv", 'a', newline='') as filedata:
                filewriter = csv.writer(filedata, delimiter=',')
                filewriter.writerow([list((listq[0]))[0], np.mean(mean2019), np.mean(mean2020)])
            return query(listq[1:])
    query(a)
if __name__ == '__main__':
    try:
        main()
    except TypeError:
        pass