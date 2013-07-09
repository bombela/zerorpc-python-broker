#!/usr/bin/env python
# bombela - 2013

import zerorpc
import gevent
import sys
import traceback

class ZerorpcWorker(object):
    def __init__(self, obj, endpoint, nb_concur_rq=1):
        self._nb_concur_rq = nb_concur_rq
        self._obj = obj
        self._broker = zerorpc.Client(endpoint)

    def run(self):
        while True:
            try:
                print 'worker is registering with',\
                        '#{0} concurrent request max...'.format(self._nb_concur_rq)
                work_queue = self._broker.worker_register(self._nb_concur_rq,
                        timeout=None)
                print 'worker is registered, waiting for work order...'
                self.worker_loop(work_queue)
            except zerorpc.LostRemote:
                print>>sys.stderr, 'lost the connection to the broker...'

    def _print_traceback(self):
        exc_infos = list(sys.exc_info())
        traceback.print_exception(*exc_infos, file=sys.stderr)
        exc_type, exc_value, exc_traceback = exc_infos
        human_traceback = traceback.format_exc()
        name = exc_type.__name__
        human_msg = str(exc_value)
        return (name, human_msg, human_traceback)

    def _async_task(self, request_id, method_name, args):
        try:
            method = getattr(self._obj, method_name)
            error = None
            try:
                result = method(*args)
                self._broker.worker_postresult(request_id, 'OK', result)
            except Exception as e:
                error = self._print_traceback()
                self._broker.worker_postresult(request_id, 'ERR', error)
        except Exception as e:
            print>>sys.stderr, 'exception:', e,\
                    'unable to process work order:',\
                    request_id, method_name, args

    def worker_loop(self, work_queue):
        for work_order in work_queue:
            gevent.spawn(self._async_task, *work_order)

class MyWorker(object):

    def add(self, a, b):
        print 'add', a, b
        return a + b

    def sleep(self, seconds):
        seconds = int(seconds)
        print 'sleep', seconds
        gevent.sleep(seconds)
        return 'I was tired'

worker = ZerorpcWorker(MyWorker(), 'tcp://127.0.0.1:9345', nb_concur_rq=16)
worker.run()
