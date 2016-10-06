import datetime
import pandas as pd
FMT_DATE_KEY = "%Y%m%d%H%M%S%f"

class TableKey:
    def __init__(self, key, time=None, freq=None):
        self.key = key
        self.freq = freq
        if self.freq and time:
            self.set_time(time)
    @classmethod
    def from_string(cls, key_str):
        key_ar = key_str.split('/')
        if len(key_ar) == 1:
            return cls(key=key_ar[0])
        elif len(key_ar) == 2:
            key, period = key_ar
            if period:
                date, freq = period.split('_')[-2:]
                return cls(key=key, time=datetime.datetime.strptime(date,FMT_DATE_KEY), freq=freq)
            else:
                return cls(key=key)
        else:
            raise Exception("Error in key_str %s"%key_str)
    def clone(self,time):
        return TableKey(key=self.key,time=time,freq=self.freq)
    def set_time(self,time):
        if self.freq and time:
            self.period = pd.Period(time, self.freq)
    def _key(self):
        if self.freq:
            return (self.key,self.period.start_time.strftime(FMT_DATE_KEY),self.period.freqstr.split("-")[0])
        else:
            return (self.key,None,None)
    def __hash__(self):
        return hash(self._key())
    def __eq__(self, other):
        return self._key() == other._key()
    def __str__(self):
        if self.freq:
            return "%s/_%s_%s"%self._key()
        else:
            return "%s"%self.key
    def start(self):
        if self.freq:
            return self.period.start_time.to_datetime()
        else:
            return None
    def end(self):
        if self.freq:
            return self.period.end_time.to_datetime()
        else:
            return None

class TableStore:
    def __init__(self, fn, data_path="Data"):
        self.store_filename = "%s/%s" % (data_path, fn)
        self._cached = {}
        self.loaded = False
    def list(self):
        with pd.HDFStore(self.store_filename) as store:
            for k, t in store.items():
                expire = None
                modified = None
                key = TableKey.from_string(k[1:])
                if hasattr(t._v_attrs, "expire_time"):
                    expire = getattr(t._v_attrs, "expire_time")
                if hasattr(t._v_attrs, "modified_time"):
                    modified = getattr(t._v_attrs, "modified_time")
                table_df = store[k]
                print "%s\t M. %s\t E. %s\t (%s ~ %s)\t %s"%(k, modified, expire, key.start(), key.end(), table_df.shape)
    def load_entries(self):
        """

        Returns
        -------
        None
        """
        with pd.HDFStore(self.store_filename) as store:
            for k, t in store.items():
                # print k
                key = TableKey.from_string(k[1:])
                expire = None
                table_df = store[k]
                if hasattr(t._v_attrs, "expire_time"):
                    expire = getattr(t._v_attrs, "expire_time")
                if not self._cached.has_key(key):
                    self._cached[key] = ()
                self._cached[key] = [table_df, expire]
            self.loaded = True
    def _is_entry_valid(self,key):
        entry = self._cached[key]
        if entry[1] is None:
            # no expiring time
            return True
        elif entry[1] > datetime.datetime.now():
            # not expired
            return True
        return False
    def is_table_valid(self, key):
        #print "check key", key
        if not self.loaded:
            self.load_entries()
        if self._cached.has_key(key):
            # table is cached
            return self._is_entry_valid(key)
        return False
    def itertables(self):
        for k, e in self._cached.iteritems():
            yield k, e[0], e[1]

    def get_table(self, key):
        """
        assumption is that the is_table_valid(key) returns true
        Parameters
        ----------
        key

        Returns
        -------

        """
        return self._cached[key][0]

    def add_table(self, key, df, **kwargs):
        """
        assumption is that the is_table_valid(key) returns false
        Parameters
        ----------
        key: table key
        df: pandas dataframe
        kwargs: timeout period

        Returns
        -------
        """
        with pd.HDFStore(self.store_filename) as store:
            # put table into store
            store.put(str(key), df)
            t = getattr(store.root, str(key))
            # update timestamp through _v_attrs[""]
            t._v_attrs["modified_time"] = datetime.datetime.now()
            if kwargs:
                t._v_attrs["expire_time"] = datetime.datetime.now() + datetime.timedelta(**kwargs)
            else:
                t._v_attrs["expire_time"] = None
            store.flush()
            self._cached[key] = [df, t._v_attrs["expire_time"]]
        return df
    def remove(self,key):
        with pd.HDFStore(self.store_filename) as store:
            store.remove(key)
            self._cached = {}
            self.load_entries()

#
# decorators
class Table(object):
    def __init__(self, f):
        self.__name__ = f.__name__
        self.function = f
        if isinstance(f, Table):
            self.child_table = f
        else:
            self.child_table = None
        self.table_metadata = {}
        if hasattr(f, "table_metadata"):
            self.table_metadata = {k:c for k,c in f.table_metadata.iteritems()}
        self._ = None
    def __call__(self, **kwargs):
        self._ = self.__call_func__(**kwargs)
        return self._
    def DF(self):
        return self()
    def join(self, other, keys, **kwargs):
        # join base table with right table
        # check if base and right table have keys
        for k in keys:
            if k not in other.columns:
                raise Exception("key %s isn't in base table"%k)
        # prepare right table
        if self.table_metadata["freq"]:
            # right table is split by period
            dfs = []
            for d in iter_table_dates(other, freq=self.table_metadata["freq"]):
                df = self(time=d,**kwargs)
                if type(df) == pd.DataFrame and len(df):
                    dfs.append(df)
            # concatenate the tables
            right = pd.concat(dfs,ignore_index=True)
        else:
            # right table is loaded by loader
            right = self(**kwargs)
        # left join base table with right table
        rt = pd.merge(other, right, how="left", on=keys)
        if kwargs.has_key("reuse_index") and kwargs["reuse_index"] is True:
            return rt.set_index(other.index)
        return rt

def Check(required):
    class _TableCheck(Table):
        def __init__(self, f):
            super(_TableCheck,self).__init__(f)
            self.table_metadata["required_columns"] = required
        def __call_func__(self, **kwargs):
            try:
                df = self.function(**kwargs)
            except Exception as e:
                print e
            if type(df) == pd.DataFrame and not df.empty:
                # check if the df has all the required columns
                for c in required:
                    if c not in df.columns:
                        print "Required Columns:", required
                        print "Loaded Columns", df.columns
                        raise Exception("""Missing required column "%s" in returned Dataframe"""%c)
                # DF is qualified to return
                return df[required]
            else:
                # return a empty DF
                return pd.DataFrame(columns=required)
    return _TableCheck
def _gen_col_decor(old, new, func):
    def _ColDecor(col=old,replace=True, as_index=False):
        class _TableDecor(Table):
            def __init__(self, f):
                super(_TableDecor, self).__init__(f)
            def __call_func__(self, **kwargs):
                df = self.function(**kwargs)
                if type(df) == pd.DataFrame and col in df.columns:
                    df[new] = func(df[col])
                    if as_index:
                        df = df.set_index(df[new])
                    cols = df.columns.tolist()
                    if replace:
                        cols.remove(col)
                    return df[cols]
                raise Exception("No %s column in Datafram"%col)
        return _TableDecor
    return _ColDecor
DateCol = _gen_col_decor("opDate", "Date", lambda d: pd.to_datetime(d,format="%Y-%m-%d"))
TickerCol = _gen_col_decor("stockCode", "Stockcode", lambda d: pd.to_numeric(d))

def TimeCat(freq):
    def _decor_wrapper(f):
        def _func_wrapper(start,end,**kwargs):
            args = {"start":start.strftime("%Y-%m-%d"),"end":end.strftime("%Y-%m-%d"),"freq":freq}
            rng = pd.date_range(**args)
            sub_frames = []
            for ts in rng:
                #print d.strftime("%Y-%m-%d")
                sub_df = f(ts=ts,**kwargs)
                #day_df = ts.sz_margin_details(d.strftime("%Y-%m-%d"))
                if len(sub_df):
                    sub_frames.append(sub_df)
            return pd.concat(sub_frames,ignore_index=True)
        _func_wrapper.__name__ = f.__name__
        return _func_wrapper
    return _decor_wrapper

def table(store, freq=None, precision="D", **experiration):
    """
    store:
    freq:
    precision: None if not a time series dataframe
    experiration:[days[, seconds[, microseconds[, milliseconds[, minutes[, hours[, weeks]]]]]]])
    Returns
    """
    class _Table(Table):
        def __init__(self,f):
            super(_Table,self).__init__(f)
            self.table_metadata["freq"] = freq
            self.table_metadata["store"] = store
            self.table_metadata["precision"] = precision
            self.table_metadata["experiration"] = experiration
            self.search_key = TableKey(key=f.__name__, freq=freq)
        def __call_func__(self, **kwargs):
            if freq:
                # periodical key
                if kwargs.has_key("time"):
                    self.search_key.set_time(kwargs['time'])
                    kwargs["start"] = self.search_key.start()
                    kwargs["end"] = self.search_key.end()
                else:
                    #error
                    raise Exception("missing time argument")
            # force reload from source
            force_reload = False
            if kwargs.has_key("force_reload"):
                force_reload = kwargs["force_reload"]
            # check if key in store
            if force_reload is False:
                if store.is_table_valid(self.search_key):
                    #print "cached"
                    return store.get_table(self.search_key)
            #print "reload", self.search_key
            kwargs["store"] = store
            df = self.function(**kwargs)
            store.add_table(self.search_key, df, **experiration)
            return df
    return _Table

def is_table_type(f):
    return hasattr(f,"table_metadata")
