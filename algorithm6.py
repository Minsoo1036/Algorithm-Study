# -*- coding: utf-8 -*-
"""
Created on Wed Jan  6 22:22:00 2021

moveHanoi

@author: pc
"""
n=int
cnt=0

def solution(depart,temp,arrive,n):
    if n==1:
        global cnt
        cnt+=1
        print("%d : 말뚝 %s에서 말뚝 %s로 원반 %d을 이동 \n" % (cnt,depart,arrive,1) )
        
    else:
        solution(depart,arrive,temp,n-1)
        cnt+=1
        print("%d : 말뚝 %s에서 말뚝 %s로 원반 %d을 이동 \n" %(cnt, depart, arrive, n) )
        solution(temp,depart,arrive,n-1)

if __name__=="__main__" :        
    solution("A","B","C",4)
    
    
print("종료하려면 1보다 작은 정수를 입력하세요.\n\n")
n=input("하노이 탑에서 옮기려는 원반의 수는? > ")

while(int(n)>0):
    solution('A','B','C',int(n))
    cnt=0
    n=input("하노이 탑에서 옮기려는 원반의 수는? > ")
