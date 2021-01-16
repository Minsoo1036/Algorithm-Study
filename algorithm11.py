class ArrayStack:

    def __init__(self):
        self.data = []

    def size(self):
        return len(self.data)

    def isEmpty(self):
        return self.size() == 0

    def push(self, item):
        self.data.append(item)

    def pop(self):
        return self.data.pop()

    def peek(self):
        return self.data[-1]

prec = {
    '*': 3, '/': 3,
    '+': 2, '-': 2,
    '(': 1
}

def solution(S):
    opStack = ArrayStack()
    answer=''
    prec = {
    '*': 3, '/': 3,
    '+': 2, '-': 2,
    '(': 1}
    
    for i in S:
        if i in prec:
            if i == "(" or opStack.isEmpty():
                opStack.push(i)
            else:
                j=prec.get(opStack.peek())
                while prec.get(i)<=j:
                    answer+=opStack.pop()
                    if opStack.isEmpty():
                        break
                    j=prec.get(opStack.peek())
                opStack.push(i)
                        
                        
                                 
        elif i==")":
            k=opStack.pop()
            while(k!="("):
                answer+=k
                k=opStack.pop()

        else:
            answer+=i
            
    while not opStack.isEmpty():
        answer+=opStack.pop()
        
    return answer