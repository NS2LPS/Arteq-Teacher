import zmq

host = "127.0.0.1"
port = "5556"

# Creates a socket instance
context = zmq.Context()
socket = context.socket(zmq.SUB)

def reload_qm(output):
    with output:
        importlib.reload(config)
        qm = qmm.open_qm(config.config)
        print(f'{time.asctime()} QM is ready with id {qm.id}')

def calibrate_qm(output,cal_qubit=True,cal_resonator=True):
    with output:
        importlib.reload(config)
        qm = qmm.open_qm(config.config)
        if cal_qubit:
            caldict = {config.qubit_LO: [config.qubit_IF,]}
            print(f'Calibrating qubit at {config.qubit_LO/1e6:.1f} + {config.qubit_IF/1e6:.1f} MHz')
            qm.calibrate_element('qubit',caldict)
            print('Done')
        if cal_resonator:
            caldict = {config.resonator_LO: [config.resonator_IF,]}
            print(f'Calibrating resonator at {config.resonator_LO/1e6:.1f} + {config.resonator_IF/1e6:.1f} MHz')
            qm.calibrate_element('resonator',caldict)
            print('Done')
        qm = qmm.open_qm(config.config)
        print(f'{time.asctime()} QM is ready with id {qm.id}')

class QueueMonitorSimple(threading.Thread):
    def __init__(self, output, QM_label, job_table, dropdown_kill):
        super().__init__()
        self.output = output
        self.job_table = job_table
        self.QM_label = QM_label
        self.keeprunning = True
    def run(self):
        while self.keeprunning:
            self.parse_queue()
            time.sleep(0.1)
        self.output.append_stdout("Done\n")
    def stop(self):
        self.keeprunning = False
    def kill(self):
        try:
            qm_list = qmm.list_open_qms()
            qm = qmm.get_qm(qm_list[0])
        except:
            qm = None
        if qm:
            job = qm.get_running_job()
            if job:
                job.halt()
    def parse_queue(self):
        try:
            qm_list = qmm.list_open_qms()
            qm = qmm.get_qm(qm_list[0])
        except:
            qm = None
        if qm:
            try:
                self.QM_label.value = f"Jobs on {qm.id}"
                table = []
                for job in qm.queue.pending_jobs:
                    table.append(f"""<tr><td>Pending</td><td>{job.id}</td></tr>""")
                job = qm.get_running_job()
                if job:
                    table.append(f"""<tr><td>Running</td><td>{job.id}</td></tr>""")
                rows = " ".join(table)
                self.job_table.value = f"<table>{rows}</table>"
            except:
                self.job_table.value = "<p>Error while parsing the queue</p>"
        else:
            self.QM_label.value = "No QM running"

__killtime__ = {"inf":1e10, "30s":30, "1min":60, "2min":120, "5min":300, }
class QueueMonitor(threading.Thread):
    def __init__(self, output, QM_label, job_table, dropdown_kill):
        super().__init__()
        self.output = output
        self.job_table = job_table
        self.QM_label = QM_label
        self.dropdown_kill = dropdown_kill
        self.keeprunning = True
        self.joblist = []
        self.jobmax = 100
        self.socket = context.socket(zmq.SUB)
        self.socket.bind(f"tcp://{host}:{port}")
        self.socket.subscribe("JOB")
    def run(self):
        poller = zmq.Poller()
        poller.register(self.socket, zmq.POLLIN)
        while self.keeprunning:
            evts = dict(poller.poll(timeout=100))
            if self.socket in evts:
                topic = self.socket.recv_string()
                job = self.socket.recv_json()
                self.joblist.append(job)
                if len(self.joblist)>self.jobmax:
                    self.joblist.pop(0)
            self.parse_queue()
        self.socket.close()
        self.output.append_stdout("Done\n")
    def stop(self):
        self.keeprunning = False
    def kill(self):
        try:
            qm_list = qmm.list_open_qms()
            qm = qmm.get_qm(qm_list[0])
        except:
            qm = None
        if qm:
            job = qm.get_running_job()
            if job:
                job.halt()
    def search_job(self,job_id,qm_id,status):
        for job in self.joblist:
            if job_id==job['id'] and qm_id==job['qm_id'] and status==job['status']:
                return job
        return dict()
    @property
    def killtime(self):
        return __killtime__[self.dropdown_kill.value]
    def parse_queue(self):
        try:
            qm_list = qmm.list_open_qms()
            qm = qmm.get_qm(qm_list[0])
        except:
            qm = None
        if qm:
            try:
                self.QM_label.value = f"Jobs on {qm.id}"
                table = []
                for job in qm.queue.pending_jobs:
                    job_local = self.search_job(job.id,qm.id,"pending")
                    waiting_time = int(time.time()-job_local['time']) if job_local else None
                    waiting_time_str = f"{waiting_time}s" if job_local else "----"
                    table.append(f"""<tr><td>Pending</td><td>{job.id}</td>
                    <td>{job_local.get("user","----")}</td>
                    <td>{waiting_time_str}</td></tr>""")
                job = qm.get_running_job()
                if job:
                    job_local = self.search_job(job.id,qm.id,"running")
                    waiting_time = int(time.time()-job_local['time']) if job_local else None
                    waiting_time_str = f"{waiting_time}s" if job_local else "----"
                    table.append(f"""<tr><td>Running</td><td>{job.id}</td>
                    <td>{job_local.get("user","----")}</td>
                    <td>{waiting_time_str}</td></tr>""")
                    if job_local and (waiting_time > self.killtime):
                        job.halt()
                rows = " ".join(table)
                self.job_table.value = f"<table>{rows}</table>"
            except:
                self.job_table.value = "<p>Error while parsing the queue</p>"
        else:
            self.QM_label.value = "No QM running"

def createQueueMonitor(*args):
    try:
        m = QueueMonitor(*args)
    except:
        print("Someone already is already listening, falling back to simple QueueMonitor")
        m = QueueMonitorSimple(*args)
    return m