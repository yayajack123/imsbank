import pymysql #import modul pymysql untuk melakukan koneksi ke database
import time #import modul time untuk waktu jeda manual

#Deklarasi Fungsi engineToko
def engineToko(after, dataUpdate):

    #deklarasi variabel
    tambahToko = 0
    kurangToko = 0
    updateToko = 0
    update =[]

    #QWERY yang digunakan
    tokoTransaksi = """SELECT id_transaksi, no_rekening,tgl_transaksi, total_transaksi,`status` FROM tb_transaksi
                    WHERE id_transaksi NOT IN(SELECT id_transaksi FROM tb_integrasi);"""
    tokoIntegrasi = """SELECT id_transaksi, rekening_pengirim, tgl_transaksi, nominal_transaksi,`status` FROM tb_integrasi
                     WHERE id_transaksi NOT IN(SELECT id_transaksi FROM tb_transaksi)"""
    tokoUpdate = """SELECT id_transaksi FROM tb_transaksi
                WHERE id_transaksi NOT IN (SELECT tb_transaksi.`id_transaksi` FROM tb_transaksi
                                            JOIN tb_integrasi
                                            WHERE tb_transaksi.`no_rekening` = tb_integrasi.`rekening_pengirim` AND 
                                                  tb_transaksi.`tgl_transaksi` = tb_integrasi.`tgl_transaksi` AND
                                                  tb_transaksi.`total_transaksi` =tb_integrasi.`nominal_transaksi` AND
                                                  tb_transaksi.`status` = tb_integrasi.`status`
                                            GROUP BY id_transaksi);"""
    dataTransaksi = "SELECT no_rekening, tgl_transaksi, total_transaksi,`status` FROM tb_transaksi WHERE id_transaksi = %s"
    dataIntegrasi = "SELECT rekening_pengirim, tgl_transaksi, nominal_transaksi,`status` FROM tb_integrasi WHERE id_transaksi = %s"

    if (len(dataUpdate) > 0 and after ==1 ):
        for data in dataUpdate:
            curToko.execute("SELECT * FROM tb_integrasi WHERE id_transaksi = %s", data)
            hasil = curToko.fetchall()
            curToko.execute("""UPDATE tb_transaksi SET no_rekening = %s, tgl_transaksi = %s, total_transaksi = %s, `status` = %s
                            WHERE id_transaksi = %s""", (hasil[0][1], hasil[0][2], hasil[0][3],hasil[0][4], hasil[0][0]))
            connToko.commit()
        print("data berhasil di update")
    #
    curToko.execute(tokoIntegrasi)
    hasilQwery = curToko.fetchall()
    if (len(hasilQwery) > 0 and after == 1) :
        print("Ada Data Baru Ditambahkan pada Transaksi Bank!")
        # curToko.execute(tokoIntegrasi)
        for data in hasilQwery:
            curToko.execute("""INSERT INTO tb_transaksi(id_transaksi, no_rekening, tgl_transaksi, total_transaksi,`status`) 
                                VALUES(%s, %s, %s, %s, %s)""",(data[0], data[1], data[2], data[3], data[4]))
            connToko.commit()

    curToko.execute(tokoTransaksi)
    hasilQwery = curToko.fetchall()
    if (len(hasilQwery) > 0 and after == 1):
        print("Ada Data Baru Dihapus pada Transaksi Bank!")
        # curToko.execute(tokoTransaksi)
        for data in hasilQwery:
            curToko.execute("DELETE FROM tb_transaksi WHERE id_transaksi = %s", data[0])
            connToko.commit()

    curToko.execute(tokoTransaksi)
    hasilQwery = curToko.fetchall()
    if (len(hasilQwery) > 0 and after == 0) :
        tambahToko=1
        print("Ada Data Baru Ditambahkan pada Transaksi Toko!")
        # curToko.execute(tokoTransaksi)
        for data in hasilQwery:
            #eksekusi QWERY insert ke tb_integrasi
            curToko.execute("""INSERT INTO tb_integrasi(id_transaksi, rekening_pengirim, tgl_transaksi, nominal_transaksi,`status`) 
                                VALUES(%s, %s, %s, %s, %s)""", (data[0], data[1], data[2], data[3], data[4]))
            connToko.commit()
            print("Data Berhasil di-Update")

    curToko.execute(tokoIntegrasi)
    hasilQwery = curToko.fetchall()
    if (len(hasilQwery) > 0 and after == 0):
        kurangToko=1
        print("Ada Data Baru Dihapus pada Transaksi Toko!")
        for data in hasilQwery:
            curToko.execute("DELETE FROM tb_integrasi WHERE id_transaksi = %s", data[0])
            connToko.commit()

    curToko.execute(tokoUpdate)
    hasilQwery = curToko.fetchall()
    if (len(hasilQwery) > 0):
        updateToko = 1
        print("Ada Data baru diupdate pada Transaksi Toko!")
        # curToko.execute(tokoUpdate)
        for data in hasilQwery:
            curToko.execute(dataTransaksi, data)
            transaksi = curToko.fetchall()
            curToko.execute(dataIntegrasi, data)
            integrasi = curToko.fetchall()
            for i in range(4):  # 0 = no_rek; 1 = tgl; 2=total
                if transaksi[0][i] != integrasi[0][i]:
                    if i == 0:
                        field = "*/rekening_pengirim/*"
                    elif i == 1:
                        field = "*/tgl_transaksi/*"
                    elif i == 2:
                        field = "*/nominal_transaksi/*"
                    else :
                        field = "*/`status`/*"
                    update.append("{}#{}#{}".format(data[0], transaksi[0][i], field))

        for i in range(len(update)):
            data = update[i].split("#")
            curToko.execute("UPDATE tb_integrasi SET /*%s*/= %s WHERE id_transaksi = %s;", (data[2], data[1], data[0]))
            connToko.commit()
        print("data berhasil di update")
    else :
        print("Tidak Ada Data Baru di Transaksi Toko")
    return tambahToko, kurangToko,updateToko,update

########################################################################################################################
#Deklarasi Fungsi engineBank
def engineBank(after, dataUpdate):
    #deklarasi Variabel
    tambahBank=0
    kurangBank=0
    updateBank=0
    update = []
    #Qwery yang digunakan pada Bank
    bankTransaksi = """SELECT id_transaksi, rekening_tujuan, tgl_transaksi, nominal_transaksi,`status` FROM db_bank.`tb_transaksi`
                    WHERE tb_transaksi.`id_transaksi` NOT IN
                    (SELECT tb_integrasi.`id_transaksi`FROM tb_integrasi);"""
    bankIntegrasi = """SELECT * FROM tb_integrasi
                     WHERE id_transaksi NOT IN(SELECT id_transaksi FROM tb_transaksi)"""
    bankUpdate = """SELECT id_transaksi FROM tb_transaksi
                WHERE id_transaksi NOT IN (SELECT tb_transaksi.`id_transaksi` FROM tb_transaksi
                                            JOIN tb_integrasi
                                            WHERE tb_transaksi.`rekening_tujuan` = tb_integrasi.`rekening_pengirim` AND 
                                                  tb_transaksi.`tgl_transaksi` = tb_integrasi.`tgl_transaksi` AND
                                                  tb_transaksi.`nominal_transaksi` =tb_integrasi.`nominal_transaksi` AND
                                                  tb_transaksi.`status` = tb_integrasi.`status`
                                            GROUP BY id_transaksi);"""
    dataTransaksi = "SELECT rekening_tujuan, tgl_transaksi, nominal_transaksi,`status` FROM tb_transaksi WHERE id_transaksi = %s"
    dataIntegrasi = "SELECT rekening_pengirim, tgl_transaksi, nominal_transaksi,`status` FROM tb_integrasi WHERE id_transaksi = %s"

    if (len(dataUpdate) > 0 and after ==1 ):
        for data in dataUpdate:
            curBank.execute("SELECT * FROM tb_integrasi WHERE id_transaksi = %s", data)
            hasil = curBank.fetchall()
            curBank.execute("""UPDATE tb_transaksi SET rekening_tujuan = %s, tgl_transaksi = %s, nominal_transaksi = %s,`status`= %s
                            WHERE id_transaksi = %s""", (hasil[0][1], hasil[0][2], hasil[0][3],hasil[0][4], hasil[0][0]))
            connBank.commit()
        print("data berhasil di update")

    #jika ada penambahan data di transaksi
    curBank.execute(bankIntegrasi)
    if len(curBank.fetchall()) > 0 and after == 1:
        print("Ada Data Baru Ditambahkan pada Transaksi Toko!")
        curBank.execute(bankIntegrasi)
        for data in curBank.fetchall():
            curBank.execute("""INSERT INTO tb_transaksi(id_transaksi, rekening_tujuan, tgl_transaksi, nominal_transaksi,`status`)
                                VALUES(%s, %s, %s, %s, %s)""", (data[0], data[1], data[2], data[3], data[4]))
            connBank.commit()

    curBank.execute(bankTransaksi)
    if (len(curBank.fetchall())> 0 and after == 1):
        print("Ada Data Baru Dihapus pada Transaksi Toko!")
        curBank.execute(bankTransaksi)
        for data in curBank.fetchall():
            curBank.execute("DELETE FROM tb_transaksi WHERE id_transaksi = %s", data[0])
            connBank.commit()

    curBank.execute(bankTransaksi)
    if len(curBank.fetchall()) > 0 and after == 0:
        tambahBank = 1
        print("Ada Data Baru Ditambahkan pada Transaksi Bank!")
        curBank.execute(bankTransaksi)
        for data in curBank.fetchall():
            curBank.execute("""INSERT INTO tb_integrasi(id_transaksi, rekening_pengirim, tgl_transaksi, nominal_transaksi,`status`) 
                                VALUES(%s, %s, %s, %s, %s)""",(data[0], data[1], data[2], data[3], data[4]))
            connBank.commit()

    curBank.execute(bankIntegrasi)
    if len(curBank.fetchall()) > 0 and after == 0:
        kurangBank = 1
        print("Ada Data Baru Dihapus pada Transaksi Bank!")
        curBank.execute(bankIntegrasi)
        for data in curBank.fetchall():
            curBank.execute("""DELETE FROM tb_integrasi WHERE id_transaksi = %s""", data[0])
            connBank.commit()

    curBank.execute(bankUpdate)
    hasilQwery = curBank.fetchall()
    if (len(hasilQwery) > 0):
        updateBank= 1
        print("Ada Data baru diupdate pada Transaksi Bank!")
        # curToko.execute(tokoUpdate)
        for data in hasilQwery:
            curBank.execute(dataTransaksi, data)
            transaksi = curBank.fetchall()
            curBank.execute(dataIntegrasi, data)
            integrasi = curBank.fetchall()
            for i in range(4):  # 0 = no_rek; 1 = tgl; 2=total
                if transaksi[0][i] != integrasi[0][i]:
                    if i == 0:
                        field = "*/rekening_pengirim/*"
                    elif i == 1:
                        field = "*/tgl_transaksi/*"
                    elif i == 2:
                        field = "*/nominal_transaksi/*"
                    else:
                        field = "*/`status`/*"
                    update.append("{}#{}#{}".format(data[0], transaksi[0][i], field))

        for i in range(len(update)):
            data = update[i].split("#")
            curBank.execute("UPDATE tb_integrasi SET /*%s*/= %s WHERE id_transaksi = %s;", (data[2], data[1], data[0]))
            connBank.commit()
        print("data berhasil di update")
    else:
        print("Tidak Ada Data Baru di Transaksi Bank")
    return tambahBank, kurangBank, updateBank, update

########################################################################################################################
#deklarasi engine sinkronisasi
def engineSingkronisasi(insertToko, hapusToko, updateToko, tokoUpdate, insertBank, hapusBank, updateBank, bankUpdate):
    tokoTransaksi = []
    bankTransaksi = []
    tokoIntegrasi = """SELECT * FROM db_toko.tb_integrasi
                    WHERE id_transaksi NOT IN(SELECT db_bank.tb_integrasi.id_transaksi FROM db_bank.tb_integrasi);"""

    bankIntegrasi = """SELECT * FROM db_bank.tb_integrasi
                     WHERE id_transaksi NOT IN(SELECT db_toko.tb_integrasi.id_transaksi FROM db_toko.tb_integrasi)"""

    if (insertToko == 1):
        print("Ada Data Baru Ditambahkan ke Integrasi Toko")
        curToko.execute(tokoIntegrasi)
        for data in curToko.fetchall():
            curBank.execute("""INSERT INTO tb_integrasi 
                               VALUES(%s, %s, %s, %s, %s)""", (data[0], data[1], data[2], data[3],data[4]))
            connBank.commit()
            print("data pada integrasi bank sudah ditambahkan")

    if(hapusToko == 1):
        print("Ada Data Baru Dihapus ke Integrasi Toko")
        curBank.execute(bankIntegrasi)
        for data in curBank.fetchall():
            curBank.execute("""DELETE FROM tb_integrasi WHERE id_transaksi = %s""", data[0])
            connBank.commit()
            print("data pada integrasi bank sudah dihapus")

    if(updateToko == 1):
        for i in range(len(tokoUpdate)):
            data = tokoUpdate[i].split("#")
            curBank.execute("UPDATE tb_integrasi SET /*%s*/= %s WHERE id_transaksi = %s;",(data[2], data[1],data[0]))
            connBank.commit()
            tokoTransaksi.append(data[0])
        print("data pada integrasi bank sudah diupdate")

    if (insertBank == 1):
        print("Ada Data yang Ditambahkan Di Transaksi Bank")
        curBank.execute(bankIntegrasi)
        for data in curBank.fetchall():
            curToko.execute("""INSERT INTO tb_integrasi 
                               VALUES(%s, %s, %s, %s, %s)""", (data[0], data[1], data[2], data[3],data[4]))
            connToko.commit()
            print("data pada integrasi toko sudah ditambahkan")
    if(hapusBank == 1):
        print("Ada Data Baru Dihapus ke Integrasi Bank")
        curToko.execute(tokoIntegrasi)
        for data in curToko.fetchall():
            curToko.execute("""DELETE FROM tb_integrasi WHERE id_transaksi = %s""", data[0])
            connToko.commit()
            print("data pada integrasi toko sudah dihapus")
    if(updateBank == 1):
        for i in range(len(bankUpdate)):
            data = bankUpdate[i].split("#")
            curToko.execute("UPDATE tb_integrasi SET /*%s*/= %s WHERE id_transaksi = %s;",(data[2], data[1],data[0]))
            connToko.commit()
            bankTransaksi.append(data[0])
        print("data pada integrasi toko sudah diupdate")
    else:
        print("Tidak Ada Perubahan pada Tabel Integrasi")

    return tokoTransaksi, bankTransaksi

while(1):
    # try:
        connToko = pymysql.connect(host='remotemysql.com', user='bYyUrZbwkl', passwd='3JEFXpZFE7', db='bYyUrZbwkl')
        connBank = pymysql.connect(host='remotemysql.com', user='RNYzmcfN9Y', passwd='gA8kVJKa0B', db='RNYzmcfN9Y')
        curToko = connToko.cursor()
        curBank = connBank.cursor()
        tokoTransaksi=[]
        bankTransaksi=[]
        #sebelum
        insertToko, hapusToko,updateToko,tokoUpdate = engineToko(0, bankTransaksi)
        insertBank, hapusBank, updateBank, bankUpdate = engineBank(0,tokoTransaksi)
        tokoTransaksi, bankTransaksi = engineSingkronisasi(insertToko, hapusToko, updateToko, tokoUpdate, insertBank, hapusBank, updateBank, bankUpdate)
        #sesudah
        engineToko(1, bankTransaksi)
        engineBank(1, tokoTransaksi)

    # except: # klo gagal tampilkan error
    #      print("ERROR!")
        time.sleep(5) # memberikan time delay selama 5 detik
