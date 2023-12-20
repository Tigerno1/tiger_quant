import os
import asyncio
from tqdm import tqdm
from multiprocessing import Pool, Process, Manager

from conf import PRODUCER_NO, CONSUMER_NO
from logger import init_logger

log = init_logger(__name__)

class SelfMultiple:
    def __init__(self, func, process: int, params: list, custom_callback=False, callback=None):
        print("==>init customized multiple class")
        self.func = func
        self.params = params
        self.process = process
        self.custom_callback = custom_callback
        self.callback = callback

    def run(self):
        self.pool = Pool(processes=self.process)

        if self.custom_callback == False:
            print("==>undefined self callback")

            pbar = tqdm(total=len(self.params))

            def update(*a):
                pbar.update()

            for param in self.params:
                result = self.pool.apply_async(self.func, param, callback=update)
                result.get()
        else:
            print("==>defined self callback")
            print(f"==>executing || {self.func}")
            for param in self.params:
                result = self.pool.apply_async(self.func, param, callback=self.callback)
                result.get()
        self.pool.close()
        self.pool.join()

class AsyncHandler:
    def run(func, iterable, *args, **kwargs):
        log.info('exec run_get_author.child process id : %s, parent process id : %s' % (os.getpid(), os.getppid()))
        tasks = [asyncio.ensure_future(func(iter, *args, **kwargs)) for iter in iterable]
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.wait(tasks))


class ProducerConsumerHandler:
    def __init__(self): 
        self.queue = Manager().Queue()
    def get_chunks(self, iterable, chunks=1):
        """
        此函数用于分割若干任务到不同的进程里去
        """
        lst = list(iterable)
        return [lst[i::chunks] for i in range(chunks)]

    # 开始爬取数据
    def run(self, producer_func, consumer_func, iterable, *args, **kwargs):
        process_count = min(PRODUCER_NO, len(iterable))
        log.info(f"process_count: {process_count}")
        log.info(f"total: {len(iterable)}")
        producer_processes = []
        for iter_item in self.get_chunks(iterable, PRODUCER_NO):
            producer_process = Process(target=producer_func, args=(iter_item, args), kwargs=kwargs)
            producer_processes.append(producer_process)
    
        # 创建多个消费者进程
        consumer_processes = []
        for i in range(CONSUMER_NO):
            consumer_process = Process(target=consumer_func)
            consumer_processes.append(consumer_process)

        # 启动生产者和消费者进程
        for producer_process in producer_processes:
            producer_process.start()
        for consumer_process in consumer_processes:
            consumer_process.start()

        # 等待生产者进程结束
        for producer_process in producer_processes:
            producer_process.join()

        # 向队列中添加结束标志
        for _ in range(len(consumer_processes)):
            self.queue.put(None)

        # 等待消费者进程结束
        for consumer_process in consumer_processes:
            consumer_process.join()



