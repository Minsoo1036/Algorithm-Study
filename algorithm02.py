# -*- coding: utf-8 -*-
"""
Created on Wed Jan  6 22:22:00 2021

indexs of specific value in array

@author: pc
"""

def solution(L, x):
    
    a=L
    answer=[]
    cp=0
    
    while x in a:
        if len(answer)==0:
            p=a.index(x)
            answer.append(p)
            a=a[p+1:]
            cp=p+1
        else:
            p=a.index(x)
            answer.append(p+cp)
            a=a[p+1:]
            cp=cp+p+1
    
    if len(answer)==0:
        answer=[-1]
        
    return answer
