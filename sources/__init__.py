# from .data_table import TableKey, TableStore
from .a_share_data import *

#
# module import hack that allows generate names on the fly
#
import sys
class _module_wrapper:
    def __init__(self, ref):
        self.ref = ref
    def __getattr__(self, name):
        # print name
        # Perform custom logic here
        try:
            return getattr(self.ref, name)
        except AttributeError as e:
            market = name[:2]
            ticker = name[-6:]
            # print market, ticker
            if market in ["SS","SZ"]:
                # shanghai or shenzheng
                return self.ref.gen_stock_loader("%s.%s"%(ticker,market))
            elif name[:2] in ["TK"]:
                #print name[-6:],name[2:4]
                return self.ref.gen_stock_tick_loader("%s.%s"%(name[-6:],name[2:4]))
            raise e
sys.modules[__name__] = _module_wrapper(sys.modules[__name__])