# -*- coding: utf-8 -*-
"""
Created on Wed Jan  6 22:22:00 2021

sorted array

@author: pc
"""

def solution(L, x):
    
    n=len(L)
    for i in range(n):
        if L[i]>x :
            break
        else:
            if i==n-1:
                i=i+1
                break
    answer=L
    answer.insert(i,x)
    return answer