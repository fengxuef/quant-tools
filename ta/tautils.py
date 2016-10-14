import pandas as pd
PrefixOpMap = {}
SuffixOpMap = {}

def _gen_operator_decor(op_map):
    def _operator_decor(argc):
        class _Op:
            def __init__(self, f):
                self._function = f
                self.name = f.__name__
                self.arg_count = argc
                if not op_map.has_key(self.name):
                    op_map[self.name] = self
            def resolve(self, df, node):
                if not hasattr(df, "__colnodes__"):
                    setattr(df, "__colnodes__", {k:None for k in df.columns})
                # resolve current node now
                fullname = node.get_nodename()
                if fullname in df.columns:
                    if df.__colnodes__[fullname] is None:
                        df.__colnodes__[fullname] = node
                    node.rowshifted = df.__colnodes__[fullname].get_rowshifted()
                else:
                    # resolve children nodes first
                    assert argc == len(node.children)
                    for c in node.children:
                        if c.is_formula():
                            c.op.resolve(df, c)
                    # prepare arguments
                    args = {}
                    nonnum_count = 1
                    for c in node.children:
                        if c.is_number():
                            args["num"] = c.number
                        else:
                            args["arg%d"%nonnum_count] = c.get_nodename()
                            nonnum_count += 1
                            if not args.has_key("col"):
                                args["col"] = c.get_nodename()
                    # print "add col:", fullname
                    ser,shft = self._function(df, **args)
                    df.loc[:,fullname] = pd.Series(ser, name = fullname, index=df.index)
                    node.rowshifted = shft
                    df.__colnodes__[fullname] = node

            def __call__(self, df, node):
                self.resolve(df, node)
        return _Op
    return _operator_decor

from collections import deque
class ColNode:
    def __init__(self,name=None,num=None,op=None,p=None,suffix=False):
        self.name = name
        self.number = num
        self.op = op
        self.suffix = suffix
        self.parent = p
        self.children = []
        self.rowshifted = 0
    def add(self, node):
        node.parent = self
        self.children.append(node)
    def is_name(self):
        return self.name is not None
    def is_number(self):
        return self.number is not None
    def is_formula(self):
        return self.op is not None
    def is_container(self):
        return self.name is None and self.number is None and self.op is None
    def argc(self):
        if self.is_formula():
            return self.op.arg_count
        return 0
    def get_rowshifted(self):
        if self.is_formula():
            rt = self.rowshifted
            for c in self.children:
                rt+=c.get_rowshifted()
            return rt
        else:
            return self.rowshifted
    def get_nodename(self):
        if self.is_number():
            return str(self.number)
        elif self.is_name():
            return self.name
        elif self.is_formula():
            rt = self.op.name
            if self.suffix:
                if self.children[0].is_number():
                    rt += self.children[0].get_nodename()
                else:
                    rt = self.children[0].get_nodename() + rt
                for c in self.children[1:]:
                    rt = c.get_nodename() + rt
            else:
                for c in self.children:
                    rt+=c.get_nodename()
            return rt
        else:
            return ""
    def __call__(self, df):
        if self.is_formula():
            self.op(df, self)
        elif self.is_name():
            if self.name not in df.columns:
                raise Exception("ERROR: %s not in dataframe"%self.name)
        elif self.is_number():
            raise Exception("ERROR: %d is not a dataframe column name"%self.number)
        else:
            raise Exception("ERROR: not a resolvable name")
    def __str__(self):
        if self.parent is None:
            # root
            return "%s = [%s]"%(self.name,",".join([str(c) for c in self.children]))
        elif self.op and self.op.arg_count == 0:
            # a operation
            return self.op.name
        elif self.is_name():
            # a column name
            return self.name
        elif self.is_number():
            # a number
            return str(self.number)
        else:
            return "%s(%s)"%(self.op.name,",".join([str(c) for c in self.children]))

import re
#name_parser = re.compile(r"""[A-Z](?:[a-z0-9]+|[A-Z0-9]*(?=[A-Z]|$))""")
#strip_parser = re.compile(r'([A-Z](?:[a-z0-9]+|[A-Z0-9]*(?=[A-Z]|$)))([A-Za-z0-9]*)')
strip_parser = re.compile(r'([A-Z](?:[a-z0-9]+|[A-Z]*[0-9]*(?=[A-Z]|$)))([A-Za-z0-9]*)')
node_parser = re.compile(r"""([a-zA-Z]+)([0-9]*)""")
def tokenize_column_name(name):
    tail = name
    while tail:
        first_split = strip_parser.findall(tail)
        if len(first_split) == 1:
            head, tail = first_split[0]
            yield head
        else:
            raise "Tokenizing ERROR: %s"%tail

def compile(name, df):
    if name in df.columns:
        return ColNode(name=name)
    # unresolvable than split name into tokens
    tok_q = deque(tokenize_column_name(name))
    # build AST from the tokens
    top = ColNode() # a container
    cur = top
    while(tok_q):
        t = tok_q.popleft()
        # print t
        if t in df.columns:
            # token is already a column name in df
            cur.add(ColNode(name=t,p=cur))
        else:
            # not in df, it's calculation
            calc_split = node_parser.findall(t)
            # print calc_split
            if len(calc_split) == 1:
                cmd, num = calc_split[0]
                if cmd in PrefixOpMap.keys():
                    argc = PrefixOpMap[cmd].arg_count
                    op = ColNode(op=PrefixOpMap[cmd],p=cur)
                    if num:
                        op.add(ColNode(num=int(num),p=op))
                    cur.add(op)
                    if argc > len(op.children):
                        cur = op
                elif cmd in SuffixOpMap.keys():
                    if cur != top:
                        # ERROR
                        raise Exception("Suffix operator met before parsing a prefix operator while parsing %s"%name)
                    argc = SuffixOpMap[cmd].arg_count
                    op = ColNode(op=SuffixOpMap[cmd],p=cur,suffix=True)
                    if num:
                        op.add(ColNode(num=int(num),p=op))
                        argc -= 1
                    for i in xrange(argc):
                        if cur.children:
                            op.add(cur.children.pop())
                        else:
                            # ERROR
                            raise Exception("Not enough node for this suffix operator %s while parsing %s"%(op,name))
                    cur.add(op)
                else:
                    # not a calculation either, unknown token
                    raise Exception("Unrecognizable token %s while parsing %s"%(t,name))
            else:
                # ERROR
                raise Exception("Parser error on %s while parsing %s"%(t,name))
        while len(cur.children) >= cur.argc():
            # move up
            if cur.parent:
                cur = cur.parent
            else:
                # cur is top
                break
    if len(top.children) == 1:
        return top.children[0]
    elif tok_q:
        # error
        raise Exception("Token left while parsing %s"%name, tok_q)
    else:
        raise Exception("Multiple nodes %s while parsing %s"%(" ".join([n.get_nodename() for n in top.children]),name))

#
#
import hashlib
class ColList(object):
    def __init__(self, names=[], n=0):
        self.period = n
        self.to_shift = n
        self._name_base_ = names
        self._name_list_ = []
        for nm in names:
            if '%' in nm:
                # template col name
                for d in xrange(0,n):
                    self._name_list_.append(nm%(d+1))
            else:
                self._name_list_.append(nm)
        #print self._name_list_
    def __len__(self):
        return len(self._name_list_)
    def __getitem__(self, item):
        return self._name_list_[item]
    def __delitem__(self, key):
        del self._name_list_[key]
    def __setitem__(self, key, value):
        self._name_list_[key] = value
        return self._name_list_[key]
    def __str__(self):
        return self.__repr__()
    def __repr__(self):
        return str(self._name_list_)
    def insert(self, key, value):
        self._name_list_.insert(key, value)
    def append(self, val):
        self._name_list_.append(val)
    def key_string(self):
        return hashlib.sha512("".join(self._name_list_)).hexdigest()
    def __process_dateframe__(self, df):
        # resolve columns in dataframe first
        for col in self._name_list_:
            #print col
            col_ast = compile(col, df)
            col_ast(df)
            to_shft = col_ast.get_rowshifted()
            #print "TO SHIFT # of rows:", str(col_ast), to_shft, self.to_shift
            if to_shft > self.to_shift:
                self.to_shift = to_shft
        return df.iloc[self.to_shift:]
    @property
    def columns(self):
        return self._name_list_
    @property
    def names(self):
        return self._name_base_

class Lookback(ColList):
    def __init__(self, cols=[], n=0):
        super(Lookback, self).__init__(cols,  n)
    def __call__(self, df):
        if df.index[0] > df.index[-1]:
            return self.__process_dateframe__(df.iloc[::-1]).iloc[::-1]
        else:
            return self.__process_dateframe__(df)

class Lookahead(ColList):
    def __init__(self, cols=[], n=0):
        super(Lookahead, self).__init__(cols,  n)
    def __call__(self, df):
        if df.index[0] < df.index[-1]:
            return self.__process_dateframe__(df.iloc[::-1]).iloc[::-1]
        else:
            return self.__process_dateframe__(df)

if __name__ == "__main__":
    """
    import re
    a = re.compile(r'((?<=[a-z0-9])[A-Z]|(?!^)[A-Z](?=[a-z]))')
    b = re.compile(r'[A-Z](?:[a-z0-9]+|[A-Z0-9]*(?=[A-Z]|$))')
    c = re.compile(r'([A-Z](?:[a-z0-9]+|[A-Z0-9]*(?=[A-Z]|$)))([A-Za-z0-9]*)')
    d = re.compile(r'(([a-zA-Z]+)([0-9]*))([A-Za-z0-9]*)')
    r = re.compile("([a-zA-Z]+)([0-9]*)")
    e = re.compile(r'([A-Z](?:[a-z0-9]+|[A-Z]*[0-9]*(?=[A-Z]|$)))([A-Za-z0-9]*)')
    """
    from sources import SS600701
    df = SS600701()
    a = compile("PctOpenClose", df)
    b = compile("MA15CloseLag5Lag10", df)
    c = compile("MA10VolumeLog5MA5CloseOpenLag2", df)
    a(df)
    b(df)