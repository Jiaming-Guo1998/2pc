from server1 import _Server
from server2 import _Client
import threading
import read_mnist as rd
import numpy as np
from multiprocessing import Semaphore,Process
from socket import *
from time import ctime
from time import time

train_images = rd.load_train_images()
train_labels = rd.load_train_labels()
# test_images = load_test_images()
# test_labels = load_test_labels()
train_images = train_images.reshape(60000,784)
X = train_images
Y = train_labels
w = np.zeros(784)
X_0 = np.random.rand(60000,784)
X_1 = X - X_0
Y_0  = np.random.rand(60000)
Y_1 = Y - Y_0
t1 = time()
G_0 = np.random.rand(2,2)
A = np.ones((2,2))
G_1 = np.dot(A,np.linalg.inv(G_0))
G_1 = G_1.T
w_0 = w_1 = w
t2 = time()
#b = np.ones(train_images.shape[0])
#X = np.insert(X, 0, values=b, axis=1)
#w[0] = 1; 
#print(w.shape)
sem = Semaphore(value=0)

i = 0 
interation = 50
alpha = 2e-7
B = 128
server_Host=''                       #服务器地址，由于我们是在同一台电脑模拟客户端和服务器通信，所以不必填服务器的地址
server_Port=21567                 #服务器端口号，最好选择非周知端口号
server_BufSize=3000000000         #缓冲区大小，单位是字节
server_Addr=(server_Host,server_Port)#生成服务器地址，主机地址加上端口号

client_Host='localhost'           #客户端类似，不再注释，localhost代指127.0.0.1，你也可以直接写127.0.0.1，这个是代指本机IP地址，
client_Port=21567
client_Bufsize=1024
client_Addr=(client_Host,client_Port)

Ser=_Server(server_Addr, server_BufSize,X_0,Y_0,G_0,B,w_0,interation,alpha,sem)  #生成一个服务器对象
Cli=_Client(client_Addr,client_Bufsize,X_1,Y_1,G_1,B,w_1,interation,alpha,sem)  #生成一个客户端对象
t3 =time()
#多线程并发运行
Cli.start()
Ser.start()
