# -*- coding: utf-8 -*-
"""
Created on Wed Jan  6 22:22:00 2021

Fibonacchi Sequence

@author: pc
"""

def solution1(x):
    if x<=1:
        answer=x
    else:
        answer=solution1(x-1)+solution1(x-2)
        
    return answer


def solution2(x):
    fl=[]
    
    for i in range(x+1):
        if i<=1:
            fl.append(i)
        else:
            fl.append(fl[i-1]+fl[i-2])
    answer=fl[-1]
    return answer    



def solution3(x):
    fl=[]
    
    for i in range(x+1):
        if i<=1:
            fl.append(i)
        else:
            a=fl[1]
            fl[1]=fl[0]+fl[1]
            fl[0]=a
            
    answer=fl[-1]
    return answer 
