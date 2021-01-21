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


def splitTokens(exprStr):
    tokens = []
    val = 0
    valProcessing = False
    for c in exprStr:
        if c == ' ':
            continue
        if c in '0123456789':
            val = val * 10 + int(c)
            valProcessing = True
        else:
            if valProcessing:
                tokens.append(val)
                val = 0
            valProcessing = False
            tokens.append(c)
    if valProcessing:
        tokens.append(val)

    return tokens


def infixToPostfix(tokenList):
    prec = {
        '*': 3,
        '/': 3,
        '+': 2,
        '-': 2,
        '(': 1,
    }

    opStack = ArrayStack()
    postfixList = []
    for i in tokenList:
        if i in prec:
            if i=="(" or opStack.isEmpty():
                opStack.push(i)
            else:
                j=prec.get(opStack.peek())
                while prec.get(i)<=j:
                    postfixList.append(opStack.pop())
                    if opStack.isEmpty:
                        break
                    j=prec.get(opStack.peek())
                opStack.push(i)
                
        elif i==")":
            k=opStack.pop()
            while(k!="("):
                postfixList.append(k)
                k=opStack.pop()
                
        else:
            postfixList.append(i)
    while not opStack.isEmpty():
        postfixList.append(opStack.pop())
                    
    return postfixList


def postfixEval(tokenList):

    opStack = ArrayStack()
    
    for i in tokenList:
        if type(i)==int:
            opStack.push(i)
        else:
            a=opStack.pop()
            b=opStack.pop() #앞에가야함.
            
            if i=="*":
                opStack.push(b*a)
            elif i=="/":
                opStack.push(b/a)
            elif i=="+":
                opStack.push(b+a)
            else:
                opStack.push(b-a)
                
    return opStack.pop()
            
            
        


def solution(expr):
    tokens = splitTokens(expr)
    postfix = infixToPostfix(tokens)
    val = postfixEval(postfix)
    return val