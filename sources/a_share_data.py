from .data_table import *
#
# predefined table store files
#
table_storage_files = {
    "ashare":TableStore("a_share.h5"),
    "macro_eco":TableStore("china_economy.h5"),
}
#
# tables
#
import tushare as ts

_rzrq_start = datetime.datetime(year=2010,month=3,day=31)
@table(store=table_storage_files["ashare"], freq="M")
@DateCol()
@Check([u'rzmre', u'rzye', u'rqmcl', u'rqyl', u'rqye', u'rzrqye', u'opDate'])
def sh_margin_general(start,end,**kwargs):
    if end < _rzrq_start:
        return None
    args = {"start":start.strftime("%Y-%m-%d"),"end":end.strftime("%Y-%m-%d")}
    return ts.sz_margins(**args)

@table(store=table_storage_files["ashare"], freq="W")
@TickerCol()
@DateCol()
@Check([u'opDate', u'stockCode', u'rzye', u'rzmre', u'rzche', u'rqyl', u'rqmcl', u'rqchl']) # w/o u'securityAbbr',
def sh_margin_details(start,end,**kwargs):
    if end < _rzrq_start:
        return None
    args = {"start":start.strftime("%Y-%m-%d"),"end":end.strftime("%Y-%m-%d")}
    return ts.sh_margin_details(**args)

@table(store=table_storage_files["ashare"], freq="M")
@DateCol()
@Check([u'opDate', u'rzye', u'rzmre', u'rqyl', u'rqylje', u'rqmcl',u'rzrqjyzl'])
def sz_margin_general(start,end,**kwargs):
    if end < _rzrq_start:
        return None
    args = {"start":start.strftime("%Y-%m-%d"),"end":end.strftime("%Y-%m-%d")}
    return ts.sh_margins(**args)

@table(store=table_storage_files["ashare"], freq="M")
@TickerCol()
@DateCol()
@Check([u'stockCode', u'rzmre', u'rzye', u'rqmcl', u'rqyl', u'rqye', u'rzrqye', u'opDate']) # w/o u'securityAbbr',
def sz_margin_details(start,end,**kwargs):
    if end < _rzrq_start:
        return None
    args = {"start":start.strftime("%Y-%m-%d"),"end":end.strftime("%Y-%m-%d"),"freq":"D"}
    rng = pd.date_range(**args)
    day_frames = []
    for d in rng:
        #print d.strftime("%Y-%m-%d")
        day_df = ts.sz_margin_details(d.strftime("%Y-%m-%d"))
        if len(day_df):
            day_frames.append(day_df)
    return pd.concat(day_frames,ignore_index=True)

@table(store=table_storage_files["macro_eco"], precision="M", days=1)
def deposit_rate(**kwargs):
    df = ts.get_deposit_rate()
    df["Date"] = pd.to_datetime(df.date, format="%Y-%m-%d")
    df["rate"] = df[["rate"]].applymap(lambda x: float(x) if x != "--" else None)["rate"]
    return df[["rate","deposit_type"]].set_index(df["Date"])

@table(store=table_storage_files["macro_eco"], precision="M", weeks=1)
def loan_rate(**kwargs):
    df = ts.get_loan_rate()
    df["Date"] = pd.to_datetime(df.date, format="%Y-%m-%d")
    df["rate"] = df[["rate"]].applymap(lambda x: float(x) if x != "--" else None)["rate"]
    return df[["rate","loan_type"]].set_index(df["Date"])

#
#
@table(store=table_storage_files["ashare"], precision=None, days=1)
def ashare_company_basics(store,**kwargs):
    df = ts.get_stock_basics()
    #df['ticker'] = df.index
    #df['ticker'] = df[['ticker']].applymap(lambda x: "%06d" % x)['ticker']
    #df['name'] = df[['name']].applymap(lambda x: x.decode('utf-8'))['name']
    #df['industry'] = df[['industry']].applymap(lambda x: x.decode('utf-8') if isinstance(x, unicode) else x)['industry']
    #df['area'] = df[['area']].applymap(lambda x: x.decode('utf-8'))['area']
    return df

#
# A-Share Stock symbols & code
#
def symbol2ticker(symbol):
    code = int(symbol)
    ticker = "%06d"%code
    if code >= 0 and code < 300000:
        return ticker, "SZ"
    elif code >= 300000 and code < 600000:
        return ticker, "SZ"
    elif code >= 600000 and code < 700000:
        return ticker, "SS"

class Fundamentals:
    def __init__(self, basics):
        self.basics_ = basics

    @property
    def name(self):
        return self.basics_["name"].decode('utf-8')
    @property
    def sybmol(self):
        return self.basics_["symbol"]
    @property
    def ticker(self):
        return self.basics_['ticker']

class Companies:
    def __init__(self, mkt, auto=False):
        if auto:
            self.load()

    def load(self):
        self.companies = ashare_company_basics()
        self.companies['symbolcode'] = self.companies.index
        self.companies['symbol'] = self.companies.index
        self.companies['symbol'] = self.companies[['symbol']].applymap(lambda x: "%06d" % int(x))['symbol']
        self.companies['ticker'] = self.companies.index
        self.companies['ticker'] = self.companies[['ticker']].applymap(lambda x: "%s.%s"%(symbol2ticker(x)))['ticker']

    def __getitem__(self, k):
        return self.companies.loc[int(k)]

    def iteritems(self):
        for row in self.companies.iteritems():
            yield Fundamentals(row.to_dict())

    def get_by_name(self,name):
        df = self.companies[self.companies.name == name]
        if df.size > 0:
            return df.iloc[0].to_dict()
        return None

    def get_by_symbol(self, symbol):
        df = self.companies[self.companies.symbol == symbol]
        if df.size > 0:
            return df.iloc[0].to_dict()
        return None

from .key_value import KeyValueStore
class SymbolCache(KeyValueStore):
    def __init__(self, name, data_path="Data"):
        super(SymbolCache, self).__init__(name, data_path)
        self.companies = Companies("all",auto=True)
    def proc_key(self, k):
        import numbers
        if isinstance(k, numbers.Number):
            return "%d"%k
        return k
    def fetch(self,k):
        v = self.companies.get_by_symbol(k)
        if v is not None:
            return Fundamentals(v).name
        return None
    def __setitem__(self, k, v):
        raise Exception("Can't setitem")
symbol_cache = SymbolCache("stock_symbols")

class NameCache(KeyValueStore):
    def __init__(self, name, data_path="Data"):
        super(NameCache, self).__init__(name, data_path)
        self.companies = Companies("all",auto=True)
    def fetch(self,k):
        v = self.companies.get_by_name(k)
        if v is not None:
            return Fundamentals(v).sybmol
        return None
    # def __setitem__(self, k, v):
    #     raise Exception("Can't setitem")
name_cache = NameCache("stock_names")


#
# Stock Data
#
import urllib2
def stockquote(ticker):
    symbol,market = ticker.split(".")
    class _TableStockQuote(Table):
        def __init__(self, f):
            super(_TableStockQuote, self).__init__(f)
            self.table_metadata["stockticker"] = ticker
        def __call_func__(self, **kwargs):
            kwargs["symbol"] = symbol
            kwargs["market"] = market
            kwargs["stockcode"] = int(symbol)
            df = self.function(**kwargs)
            df["Stockcode"] = int(symbol)
            if kwargs.has_key("date_asc") and kwargs["date_asc"] is True:
                # ascending date ordering, most recent date at last
                # default is descending order
                if df.index[0] > df.index[-1]:
                    return df.iloc[::-1]
            return df
    return _TableStockQuote

def gen_stock_tick_loader(ticker):
    @stockquote(ticker=ticker)
    @table(TableStore("stock_%s.h5"%ticker), freq='B')
    def tick_data(symbol, start, **kwargs):
        df = ts.get_tick_data(symbol,date=start.strftime("%Y-%m-%d"))
        df["Date"] = start
        df["Datetime_str"] = start.strftime("%Y-%m-%d") + " " + df["time"]
        df["Datetime"] = pd.to_datetime(df["Datetime_str"], format="%Y-%m-%d %H:%M:%S")
        return df

    return tick_data

def gen_stock_loader(ticker):
    historical_url = "http://ichart.finance.yahoo.com/table.csv?s=%s" % ticker
    quote_url = "http://finace.yahoo.com/d/quotes.csv?s=%s&f=abo" % ticker
    @stockquote(ticker=ticker)
    @table(TableStore("stock_%s.h5"%ticker), days=1)
    def historical_quote(store,**kwargs):
        #print historical_url
        df = pd.DataFrame.from_csv(urllib2.urlopen(historical_url))
        df["Date"] = pd.to_datetime(df.index, format="%Y-%m-%d")
        df = df[df['Volume'] != 0]
        if kwargs.has_key("keeporigincolumns") is False or kwargs["keeporigincolumns"] is False:
            df.rename(columns={'Adj Close':'Adj'}, inplace=True)
        else:
            df["Adj"] = df['Adj Close']
        return df.set_index(df["Date"])

    return historical_quote

def gen_stock_index_loader(ticker):
    @table(TableStore("index_%s.h5"%ticker), days=1)
    def historical_quote(store,**kwargs):
        df = ts.get_hist_data(ticker)[["open","close","high","low","volume"]]
        df.columns = ["Open","Close","High","Low","Volume"]
        df["Date"] = pd.to_datetime(df.index, format="%Y-%m-%d")
        return df.set_index(df["Date"])

    return historical_quote
