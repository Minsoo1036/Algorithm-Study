# -*- coding: utf-8 -*-
"""
Created on Wed Jan  6 22:22:00 2021

array binary search

@author: pc
"""

def solution(L, x):
    lower=0
    upper=len(L)-1
    idx=-1
    while lower<upper:
        middle = (lower+upper)//2
        if L[middle]==x:
            answer=middle
            return answer
        elif L[middle]<x:
            lower=middle+1
        else :
            upper=middle
    if L[lower]==x:
        answer=lower
    else:
        answer=idx
    
    return answer

