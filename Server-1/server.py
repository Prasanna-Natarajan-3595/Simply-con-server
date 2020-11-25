import socket
import threading
import psycopg2
from cryptography.fernet import Fernet
import os


class var:
    host = '0.0.0.0'
    port = 4983
    key = os.getenv('key')
    hosts = os.getenv('host')
    db = os.getenv('db')
    user = os.getenv('user')
    password = os.getenv('password')
    address = []
    connection = []
    print_ram = []
    ram = []
    times = ''
    name = []


var.print_ram.append('Initializing...')
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((var.host, var.port))
s.listen(100)




con = psycopg2.connect(database=var.db, user=var.user,
                      password=var.password, host=var.hosts)
#con = psycopg2.connect(user=var.hosts,password=var.password,database=var.db,host=var.hosts)
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

# This sends commands by the db and send commands to target using sensor
def execute_db_sensor():
    var.print_ram.append('DB_sensor | started')
    while True:
        try:
            try:
                cur.execute("SELECT sensor_host,id FROM data ORDER BY id")
                valss = cur.fetchall()
            except:
                continue

            for i in valss:
                for no, e in enumerate(i[0]):

                    host = e
                    try:
                        for e, _ in enumerate(var.address):
                            if _ == host:
                                    val = var.connection[e].recv(1024)
                                    f = Fernet(var.key)
                                    decrypted_message = f.decrypt(val).decode()
                                    if len(decrypted_message) > 0:
                                        var.print_ram.append(f'DB_sensor | value received | {decrypted_message} | from {host}')
                                        var.ram.append([decrypted_message, host])

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


printer_thread = threading.Thread(target=printer)
printer_thread.daemon = True
printer_thread.start()

accept_connection = threading.Thread(target=accept_conn)
accept_connection.daemon = True
accept_connection.start()

sensor = threading.Thread(target=sensor)
sensor.daemon = True
sensor.start()


execute_db_sensor()
