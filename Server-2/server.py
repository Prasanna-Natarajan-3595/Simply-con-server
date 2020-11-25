import socket
import threading
import psycopg2
import datetime
import pytz
from cryptography.fernet import Fernet
import os



class var:
    host = ''
    port_2 = 3675
    key = '0K-9VkXgzYFQz6_wG_UDFWlqRlbCj_R9LUH_CCjtlyo='
    hosts = 'ec2-18-235-109-97.compute-1.amazonaws.com'
    db = 'dal03e016sa4tr'
    user = 'xgheklawiqwvfd'
    password = '21603b4c8075e1baf5cdb8e81c3f39d833896842abfcb4c2426cc4a2afe46b60'
    addresst = []
    connectiont = []
    namet = []
    print_ram = []
    ram = []
    times = ''

t = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
t.bind((var.host, var.port_2))
t.listen(100)
con = psycopg2.connect(database=var.db, user=var.user,
                      password=var.password, host=var.hosts)
cur = con.cursor()

var.print_ram.append('Initializing successful')


# This prints the output value of threads
def printer():
    while True:
        for r in var.print_ram:
            print(r)
            var.print_ram.remove(r)

def accept_conn_t():
    var.print_ram.append('Accepting connection timing | started')
    while True:
        try:
            conn, addr = t.accept()
            var.print_ram.append('Accepting connection timing | connecting...')
            pas = conn.recv(1024).decode()
            var.print_ram.append(f'Accepting connection timing | password received | {pas}')
            if pas == 'afhihytu49t6475673':
                conn.send(bytes('hfoahguaytfas235425@#$@', 'utf-8'))
                val = conn.recv(1024)
                f = Fernet(var.key)
                decrypted_message = f.decrypt(val).decode()
                var.print_ram.append(f'Accepting connection timing | Connection successful | {decrypted_message}')
                t.setblocking(True)
                var.connectiont.append(conn)
                var.addresst.append(addr[0])

                var.namet.append(decrypted_message)
            else:
                conn.close()
                var.print_ram.append(f'Accepting connection timing | closed connection with | {addr}')

        except Exception as e:
            var.print_ram.append(f'Accepting connection timing | error | {e}')


# This verifies the db and send commands to target using time
def execute_db_timing():
    var.print_ram.append('DB_timing | started')
    while True:
      try:
        try:
            cur.execute("SELECT id,name,email,password,timing_host,timing_value,timing_time,hostname FROM data ORDER BY id")
            valas = cur.fetchall()
        except:
            continue

        for e in valas:
            for no, i in enumerate(e[4]):

                host = i
                value = e[5][no]
                hostname = e[7][no]
                tim = int(e[6][no])
                IST = pytz.timezone('Asia/Kolkata')
                datetime_ist = datetime.datetime.now(IST)
                now = int(datetime_ist.strftime('%H%M'))
                if var.times == [host, value, tim]:
                  pass
                else:
                    if tim == now:
                        if host in var.addresst:
                            try:
                               for e,_ in enumerate(var.addresst):
                                   if _ == host:
                                       if var.namet[e] == hostname:
                                           encoded_message = value.encode()
                                           f = Fernet(var.key)
                                           encrypted_message = f.encrypt(encoded_message)
                                           var.connectiont[e].send(bytes(encrypted_message.decode(), 'utf-8'))
                                           var.times = [host, value, tim]
                                           var.print_ram.append(f'DB_timing | sent {host} | value {value}')


                            except Exception:
                                var.print_ram.append(f'DB_timing | removing {host}')
                                var.connectiont.remove(var.connectiont.index(host))
                                var.addresst.remove(host)
                        else:
                            pass
                    else:
                        pass


      except Exception as ms:
          if str(ms).replace(' ','') == 'tupleindexoutofrange':
              pass
          else:
              var.print_ram.append(f'execute_db_timing | An error occured | {ms}')


accept_connectiont = threading.Thread(target=accept_conn_t)
accept_connectiont.daemon = True
accept_connectiont.start()

execute_db_timing()
