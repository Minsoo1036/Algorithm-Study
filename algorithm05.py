# -*- coding: utf-8 -*-
"""
Created on Wed Jan  6 22:22:00 2021

Binary Search (example of recursive algorithm)

@author: pc
"""
print("test")
def solution(L, x, l, u):
    if l>u: 
        return -1
    mid = (l + u) // 2
    if x == L[mid]:
        return mid
    elif x < L[mid]:
        return solution(L,x,l,mid-1)
    else:
        return solution(L,x,mid+1,u) #testing!!


if __name__=="__main__":
    L=[2,5,7,9,11]
    print(solution(L,4,0,4))
    print(solution(L,2,0,4))
