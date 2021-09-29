#服务器类

from socket import *;        #套接字模块
from time import ctime, time;      #时间模块，用来打印时间
import threading;            #线程模块
import numpy as np
import pickle
from multiprocessing import Semaphore,Process
import sys
end = bytes('over','utf-8')
class _Server(threading.Thread):
    def __init__(self, Addr, BufSize,X,Y,G,B,w,interation,alpha,sem):                        #Addr:服务器ip地址
        threading.Thread.__init__(self);                      #初始化线程
        self.tcpSerSock=socket(AF_INET,SOCK_STREAM);          #AF_INET代表生成面向网络的套接字，SOCK_STREAM代表TCP类型
        self.tcpSerSock.bind(Addr);                           #将服务器ip地址与套接字绑定
        self.tcpSerSock.listen(5);                            #设置该服务器最多建立5个TCP连接
        self.BufSize=BufSize;                                 #设置缓冲区大小
        self.X = X
        self.Y = Y
        self.G = G
        self.B = B
        self.w = w
        self.interation = interation
        self.alpha = alpha
        self.sem = sem
    def receive(self,tcpCliSock):
        data = bytes()
        while True:
            data_temp=tcpCliSock.recv(self.BufSize); 
            if end in data_temp: 
                data= data + data_temp.rstrip(end)
                break
            data = data + data_temp
        return pickle.loads(data) 
    def run(self):                                           #多线程的启动函数，只能命名为run()
        while True:
            print("waiting for connection...")
            tcpCliSock,addr=self.tcpSerSock.accept()        #accept()函数接收到客户端连接申请后，将切换消息，从而让出总线，tcpCliSock就是接线员，addr是客户端的IP地址
            print('...connected from:',addr)
            
            for j in range(self.interation):
                i = 0
                t1 = time()
                
                while True:
                    E_0 = np.dot(self.G[0,0],self.X[i:self.B])
                    E_1 = np.dot(self.G[1,0],self.X[i:self.B])
                    F_0 = np.dot(self.G[0,0],self.w)
                    F_1 = np.dot(self.G[1,0],self.w)
                    
                    tcpCliSock.sendall(pickle.dumps(E_1)+end)#1
                    # print("here")
                    self.sem.acquire()
                    tcpCliSock.sendall(pickle.dumps(F_1)+end)#2
                    
                    print(j)
                    
                    E = self.receive(tcpCliSock)
        
                    self.sem.release()
                    F = self.receive(tcpCliSock)
                    
                    # print("1:",j)
                

                    N = E_0 + E
                 
                    N_ = F_0 + F
                    Y_ = np.dot(N_,N.T)
                    D = Y_ - self.Y[i:self.B]
                   
                    E_0 = np.dot(self.G[0,0],self.X[i:self.B].T)
                    E_1 = np.dot(self.G[1,0],self.X[i:self.B].T)
                    F_0 = np.dot(self.G[0,0],D[i:self.B])
                    F_1 = np.dot(self.G[1,0],D[i:self.B])
                    
                    tcpCliSock.sendall(pickle.dumps(E_1)+end)
                    self.sem.acquire()
                    tcpCliSock.sendall(pickle.dumps(F_1)+end)
                    
                    E = self.receive(tcpCliSock)
                    self.sem.release()          #客户端发来的消息存入data中
                    F = self.receive(tcpCliSock)
                
                    P = E_0 + E
                    P_ = F_0 + F
                    theta = np.dot(P_,P.T)
                    self.w = self.w - np.dot((self.alpha/self.B),theta)
                    i = i + self.B
                    if(self.B > self.Y.shape[0] - i ):
                        break
                t2 = time()
                print(t2 - t1)

            tcpCliSock.close()                               #关闭接线员通道
            break
        self.tcpSerSock.close()                                   #关闭TCP连接
        
        

