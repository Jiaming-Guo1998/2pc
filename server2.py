from socket import *
from struct import pack;
from time import ctime;
import threading;
import numpy as np
import pickle
from multiprocessing import Semaphore,Process
import sys
end = bytes('over','utf-8')
class _Client(threading.Thread):
    def __init__(self,Addr,BufSize,X,Y,G,B,w,interation,alpha,sem):
        threading.Thread.__init__(self)
        self.tcpCliSock=socket(AF_INET,SOCK_STREAM)
        self.tcpCliSock.connect(Addr)
        self.BufSize=BufSize
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
    def run(self):
        for j in range(self.interation):
            i = 0
            while True:
                
                E_0 = np.dot(self.G[0,1],self.X[i:self.B])
                E_1 = np.dot(self.G[1,1],self.X[i:self.B])
                F_0 = np.dot(self.G[0,1],self.w)
                F_1 = np.dot(self.G[1,1],self.w)

                E = self.receive(self.tcpCliSock)#1
                
                self.sem.release()  
                F = self.receive(self.tcpCliSock)#2

                self.tcpCliSock.sendall(pickle.dumps(E_0)+end)
                self.sem.acquire()
                self.tcpCliSock.sendall(pickle.dumps(F_0)+end)
                
                
                N = E_0 + E
                N_ = F_0 + F
                Y_ = np.dot(N_,N.T)
                D = Y_ - self.Y[i:self.B]
                
                E_0 = np.dot(self.G[0,1],self.X[i:self.B].T)
                E_1 = np.dot(self.G[1,1],self.X[i:self.B].T)
                F_0 = np.dot(self.G[0,0],D[i:self.B])
                F_1 = np.dot(self.G[1,1],D[i:self.B])
                E = self.receive(self.tcpCliSock)
                self.sem.release()
                         
                F = self.receive(self.tcpCliSock)

                
                self.tcpCliSock.sendall(pickle.dumps(E_1)+end)
                self.sem.acquire()
                
                self.tcpCliSock.sendall(pickle.dumps(F_1)+end)
                
                P = E_0 + E
                P_ = F_0 + F
                theta = np.dot(P_,P.T)
                self.w = self.w - np.dot((self.alpha/self.B),theta)
                i = i + self.B
                if(self.B > self.Y.shape[0] - i ):
                    break
        self.tcpCliSock.close()

