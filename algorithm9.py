class Node:

    def __init__(self, item):
        self.data = item
        self.prev = None
        self.next = None


class DoublyLinkedList:

    def __init__(self):
        self.nodeCount = 0
        self.head = Node(None)
        self.tail = Node(None)
        self.head.prev = None
        self.head.next = self.tail
        self.tail.prev = self.head
        self.tail.next = None


    def reverse(self):
        L=[]
        curr=self.tail
        while curr.prev.prev:
            L.append(curr.prev.data)
            curr=curr.prev
        return L

    def traverse(self):
        result = []
        curr = self.head
        while curr.next.next:
            curr = curr.next
            result.append(curr.data)
        return result


    def getAt(self, pos):
        if pos < 0 or pos > self.nodeCount:
            return None

        if pos > self.nodeCount // 2:
            i = 0
            curr = self.tail
            while i < self.nodeCount - pos + 1:
                curr = curr.prev
                i += 1
        else:
            i = 0
            curr = self.head
            while i < pos:
                curr = curr.next
                i += 1

        return curr


    def insertAfter(self, prev, newNode):
        next = prev.next
        newNode.prev = prev
        newNode.next = next
        prev.next = newNode
        next.prev = newNode
        self.nodeCount += 1
        return True


    def insertAt(self, pos, newNode):
        if pos < 1 or pos > self.nodeCount + 1:
            return False

        prev = self.getAt(pos - 1)
        return self.insertAfter(prev, newNode)


    def insertBefore(self, next, newNode):
        prev=next.prev
        newNode.prev=prev
        newNode.next=next
        prev.next=newNode
        next.prev=newNode
        self.nodeCount+=1
        return True
    
    def popAfter(self, prev):
        curr=prev.next
        answer=curr.data
        prev.next=curr.next
        curr.next.prev=prev
        del curr
        self.nodeCount-=1
        return answer


    def popBefore(self, next):
        curr=next.prev
        answer=curr.data
        next.prev=curr.prev
        curr.prev.next=next
        del curr
        self.nodeCount-=1
        return answer


    def popAt(self, pos):
        if pos<1 or pos>self.nodeCount:
            raise IndexError
        else:
            Node=self.getAt(pos-1)
            answer=self.popAfter(Node)
            return answer

    def concat(self, L):
        tail=self.tail
        head=L.head
        tail.prev.next=head.next
        head.next.prev=tail.prev
        del tail
        del head
        self.nodeCount+=L.nodeCount
        self.tail=L.tail
