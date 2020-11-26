import socket
import threading
import psycopg2
from cryptography.fernet import Fernet
import os
import datetime
import pytz

class var:
    host = '0.0.0.0'
    port = 4983
    port_2 = 3675
    key = '0K-9VkXgzYFQz6_wG_UDFWlqRlbCj_R9LUH_CCjtlyo='
    hosts = 'ec2-18-235-109-97.compute-1.amazonaws.com'
    db = 'dal03e016sa4tr'
    user = 'xgheklawiqwvfd'
    password = '21603b4c8075e1baf5cdb8e81c3f39d833896842abfcb4c2426cc4a2afe46b60'
    address = []
    connection = []
    print_ram = []
    ram = []
    times = ''
    name = []
    addresst = []
    connectiont = []
    namet = []
    addresss = []
    connections = []
    names = []
    addressst = []
    connectionst = []
    namest = []


var.print_ram.append('Initializing...')
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((var.host, var.port))
s.listen(100)
cs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
cs.bind((var.host, 4982))
cs.listen(100)
t = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
t.bind((var.host, var.port_2))
t.listen(100)
ct = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
ct.bind((var.host, 3674))
ct.listen(100)



con = psycopg2.connect(database=var.db, user=var.user,
                       password=var.password, host=var.hosts)
#con = psycopg2.connect(database='simplycon',user='postgres',password='2005')
cur = con.cursor()

var.print_ram.append('Initializing successful')


# This prints the output value of threads
def printer():
    while True:
        for r in var.print_ram:
            print(r)
            var.print_ram.remove(r)


# This accepts new connection
def accept_conn():
    var.print_ram.append('Accepting connection | started')
    while True:
        try:
            conn, addr = s.accept()
            var.print_ram.append('Accepting connection | connecting...')
            conn.settimeout(5.0)
            pas = conn.recv(1024).decode()
            var.print_ram.append(f'Accepting connection | password received | {pas}')
            if pas == 'afhihytu49t6475673':
                conn.send(bytes('hfoahguaytfas235425@#$@', 'utf-8'))
                val = conn.recv(1024)
                f = Fernet(var.key)
                decrypted_message = f.decrypt(val).decode()
                var.print_ram.append(f'Accepting connection | Connection successful | {decrypted_message}')
                s.setblocking(True)
                var.connection.append(conn)
                var.address.append(addr[0])

                var.name.append(decrypted_message)
            else:
                conn.close()
                var.print_ram.append(f'Accepting connection | closed connection with | {addr}')

        except Exception as e:
            var.print_ram.append(f'Accepting connection | error | {e}')
        try:
            conn, addr = cs.accept()
            var.print_ram.append('Accepting connection | connecting...')
            conn.settimeout(5.0)
            pas = conn.recv(1024).decode()
            var.print_ram.append(f'Accepting connection | password received | {pas}')
            if pas == 'afhihytu49t6475673':
                conn.send(bytes('hfoahguaytfas235425@#$@', 'utf-8'))
                val = conn.recv(1024)
                f = Fernet(var.key)
                decrypted_message = f.decrypt(val).decode()
                var.print_ram.append(f'Accepting connection | Connection successful | {decrypted_message}')
                s.setblocking(True)
                var.connections.append(conn)
                var.addresss.append(addr[0])

                var.names.append(decrypted_message)
            else:
                conn.close()
                var.print_ram.append(f'Accepting connection | closed connection with | {addr}')

        except Exception as e:
            var.print_ram.append(f'Accepting connection | error | {e}')

# This sends commands by the db and send commands to target using sensor
def execute_db_sensor():
    var.print_ram.append('DB_sensor | started')
    while True:
        try:
            try:
                cur.execute("SELECT sensor_host,id,hostname_sensor FROM data ORDER BY id")
                valss = cur.fetchall()
            except:
                continue
            for i in valss:
                for no, e in enumerate(i[0]):

                    host = e
                    hostname = i[2][no]
                    try:
                        for e, _ in enumerate(var.address):
                            if _ == host:
                                    if var.name[e] == hostname:
                                        var.connection[e].settimeout(5.0)
                                        val = var.connection[e].recv(1024)

                                        f = Fernet(var.key)
                                        decrypted_message = f.decrypt(val).decode()
                                        if len(decrypted_message) > 0:
                                            var.print_ram.append(f'DB_sensor | value received | {decrypted_message} | from {host}')
                                            var.ram.append([decrypted_message, host])

                                        else:
                                            pass
                            else:
                                pass
                    except:
                        pass

        except Exception as e:
            if str(e) == "'int' object is not iterable":
                pass
            else:
                var.print_ram.append(f'DB_sensor | got an exception Error: {e}')


def sensor():
    while True:
      try:
        for r in var.ram:
            
            cur.execute("SELECT sensor_host,sensor_value,sensor_alt,sensor_send,hostname_sensor FROM data ORDER BY id")
            val = cur.fetchall()
            for i in val:
                for n,s in enumerate(i[0]):
                    if s == r[1]:
                        if i[1][n] == r[0]:
                            for e, _ in enumerate(var.address):
                                if _ == i[3][n]:
                                    if var.name[e] == i[4][n]:
                                        try:
                                            encoded_message = str(i[2][n]).encode()
                                            f = Fernet(var.key)
                                            encrypted_message = f.encrypt(encoded_message)
                                            var.connection[e].send(bytes(encrypted_message.decode(), 'utf-8'))
                                            var.print_ram.append(f'Sensor | Data successfully sent | {i[2][n]} | {i[4][n]}')
                                        except:
                                            var.connection.remove(e)
                                            var.print_ram.append(f'Sensor | Client removed | {e}')
            var.ram.remove(r)
      except Exception as s:
          pass

def accept_conn_t():
    var.print_ram.append('Accepting connection timing | started')
    while True:
        try:
            conn, addr = t.accept()
            var.print_ram.append('Accepting connection timing | connecting...')
            conn.settimeout(5.0)
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
        try:
            conn, addr = ct.accept()
            var.print_ram.append('Accepting connection timing | connecting...')
            conn.settimeout(5.0)
            pas = conn.recv(1024).decode()
            var.print_ram.append(f'Accepting connection timing | password received | {pas}')
            if pas == 'afhihytu49t6475673':
                conn.send(bytes('hfoahguaytfas235425@#$@', 'utf-8'))
                val = conn.recv(1024)
                f = Fernet(var.key)
                decrypted_message = f.decrypt(val).decode()
                var.print_ram.append(f'Accepting connection timing | Connection successful | {decrypted_message}')
                t.setblocking(True)
                var.connectionst.append(conn)
                var.addressst.append(addr[0])

                var.namest.append(decrypted_message)
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
                        var.print_ram.append(f'DB_timing | {var.addresst} | addresses')
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
    var.print_ram.append('execute_db_timing | Exited')

def contactor():
    while True:
        try:
         for no,i in enumerate(var.connections):
             try:
                 i.send('pulse'.encode())
                 i.settimeout(5.0)
                 i.recv(1024)
             except:
                 var.print_ram.append(f'Contactor | closed connection with | {var.names[no]} | {var.addresss[no]}')
 
                 var.names.remove(var.names[no])
                 var.addresss.remove(var.addresss[no])
                 var.connection.remove(var.connection[no])
                 var.connections.remove(i)
                 var.name.remove(var.name[no])
                 var.address.remove(var.address[no])
         for no2,i2 in enumerate(var.connectionst):
             try:
                 i2.send('pulse')
                 i2.settimeout(5.0)
                 i2.recv(1024)
             except:
                 var.print_ram.append(f'Contactor time | closed connection with | {var.namest[no2]} | {var.addresss[no2]}')
                 var.connectiont.remove(var.connectiont[no2])
                 var.connectionst.remove(i2)
                 var.namest.remove(var.names[no2])
                 var.addressst.remove(var.addresss[no2])

                 var.namet.remove(var.name[no2])
                 var.addresst.remove(var.address[no2])
        except:
            pass


accept_connectiont = threading.Thread(target=accept_conn_t)
accept_connectiont.daemon = True
accept_connectiont.start()


printer_thread = threading.Thread(target=printer)
printer_thread.daemon = True
printer_thread.start()

accept_connection = threading.Thread(target=accept_conn)
accept_connection.daemon = True
accept_connection.start()

sensor = threading.Thread(target=sensor)
sensor.daemon = True
sensor.start()

db_timings = threading.Thread(target=execute_db_timing)
db_timings.daemon = True
db_timings.start()

h = threading.Thread(target=contactor)
h.daemon = True
h.start()

execute_db_sensor()
