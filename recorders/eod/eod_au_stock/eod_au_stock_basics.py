
class EodUStockListRecorder:

    def run(self):
        api.get_eod_us_stock_list(exchanges="us")