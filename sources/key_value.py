# key/value cache store
import shelve
class KeyValueStore(object):
    def __init__(self, name, data_path, ext="cache"):
        self.cache_fn = "%s/%s.%s"%(data_path, name, ext)
        # print self.cache_fn
        self.map_ = {}
        self._dirty = False
        self._loaded = False
    def load(self):
        db_ = shelve.open(self.cache_fn)
        for k in db_.keys():
            self.map_[k] = db_[k]
        db_.close()
        self._loaded = True
    def flush(self):
        if self._dirty:
            db_ = shelve.open(self.cache_fn)
            for k, v in self.map_.iteritems():
                #print k, v
                db_[k] = v
            db_.close()
            self._dirty = False
    def proc_key(self, k):
        if isinstance(k, unicode):
            return k.encode("utf-8")
        return k
    def has_key(self,k):
        if self._loaded is False:
            self.load()
        srch_key = self.proc_key(k)
        if self.map_.has_key(srch_key) is False:
            v = self.fetch(srch_key)
            if v is None:
                return False
            self._dirty = True
            self.map_[srch_key] = v
        return True
    def clear(self):
        self.map_.clear()
        self._dirty = False
        db_ = shelve.open(self.cache_fn)
        db_.clear()
        db_.close()
    def fetch(self,k):
        return None
    def __contains__(self, k):
        return self.has_key(k)
    def __getitem__(self, k):
        return self.map_[self.proc_key(k)]
    def __setitem__(self, k, v):
        self.map_[self.proc_key(k)] = v
        self._dirty = True
    def __del__(self):
        self.flush()
