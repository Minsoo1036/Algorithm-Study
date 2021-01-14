# -*- coding: utf-8 -*-
"""
Spyder Editor
Linked List
This is a temporary script file.
"""

class Node:

    def __init__(self, item):
        self.data = item
        self.next = None


class LinkedList:

    def __init__(self):
        self.nodeCount = 0
        self.head = None
        self.tail = None


    def getAt(self, pos):
        if pos < 1 or pos > self.nodeCount:
            return None

        i = 1
        curr = self.head
        while i < pos:
            curr = curr.next
            i += 1

        return curr


    def insertAt(self, pos, newNode):
        if pos < 1 or pos > self.nodeCount + 1:
            return False

        if pos == 1:
            newNode.next = self.head
            self.head = newNode

        else:
            if pos == self.nodeCount + 1:
                prev = self.tail
            else:
                prev = self.getAt(pos - 1)
            newNode.next = prev.next
            prev.next = newNode

        if pos == self.nodeCount + 1:
            self.tail = newNode

        self.nodeCount += 1
        return True


    def popAt(self, pos):
        if pos<1 or pos>self.nodeCount:
            raise IndexError
        else:
            if pos==1 and self.nodeCount==1:
                tmp=self.head
                answer=tmp.data
                self.head = None
                self.tail = None
                del tmp
                
            elif pos==1 and self.nodeCount>1:
                tmp=self.head
                answer=tmp.data
                self.head = tmp.next
                del tmp

            elif pos==self.nodeCount:
                prev=self.getAt(pos-1)
                tmp=prev.next
                answer=tmp.data
                prev.next=tmp.next
                self.tail=prev
                del tmp

            else:
                prev=self.getAt(pos-1)
                tmp=prev.next
                answer=tmp.data
                prev.next=tmp.next
                del tmp
        self.nodeCount-=1
        return answer


    def traverse(self):
        result = []
        curr = self.head
        while curr is not None:
            result.append(curr.data)
            curr = curr.next
        return result


def solution(x):
    return 0