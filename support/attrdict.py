import types

class attrdict(object):
    """A dict whose items can also be accessed as member variables."""
    
    def __init__(self, data):
        self.__dict = data
    
    
    def __repr__(self):
        return self.__dict.__repr__()
    
    
    def __getattr__(self, key):
        val = self.__dict[key]
        
        if type(val) == types.DictType:
            val = attrdict(val)
        
        return val
    
    
    def __getitem__(self, item):
        val = self.__dict[item]
        
        if type(val) == types.DictType:
            val = attrdict(val)
        
        return val
    
    
    def __iter__(self):
        return self.__dict.__iter__()
    
    
    def iterkeys(self):
        return self.__dict.iterkeys()
    

