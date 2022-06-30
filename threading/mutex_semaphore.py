import threading
import time

mutex = threading.Lock()  # is equal to threading.Semaphore(1)

def fun1():
    while True:
        with mutex:
            print(1)
        time.sleep(.5)

def fun2():
    while True:
        with mutex:
            print(2)
        time.sleep(.5)

t1 = threading.Thread(target=fun1).start()
t2 = threading.Thread(target=fun2).start()