import ipywidgets as widgets
from IPython.display import display, HTML
import time
import threading
import os
import importlib
from qm import QuantumMachinesManager
import zmq

# QM address
QM_Router_IP = "129.175.113.167"
cluster_name = "Cluster_1"
qmm = QuantumMachinesManager(host=QM_Router_IP, cluster_name=cluster_name, log_level="ERROR", octave_calibration_db_path=os.getcwd()) 

# Local address for queue monitoring
host = "127.0.0.1"
port1 = "5556"
port2 = "5557"
context = zmq.Context()
try:
    socket2 = context.socket(zmq.PUB)
    socket2.bind(f"tcp://{host}:{port2}")
except:
    socket2 = None

def get_config(full=False):
    """Return configuration dictionary of the current QM.

    Optional argument:
    full=False : if False, only LO and IF parameters are returned"""
    qm_list = qmm.list_open_qms()
    qm = qmm.get_qm(qm_list[0])
    config = qm.get_config()
    if full:
        return config
    out = dict()
    for k,v in config['elements'].items():
        if not k.startswith("__"):
            if "mixInputs" in v:
                out[k] = {"LO":v["mixInputs"]["lo_frequency"], "IF":v["intermediate_frequency"]}
    return out   
    
def show_config():
    """Display the configuration of the current QM."""
    try:
        qm_list = qmm.list_open_qms()
        qm = qmm.get_qm(qm_list[0])
        config = qm.get_config()
    except:
        return "Could not find running QM"
    s = f"<h2>Configuration of {qm.id}</h2><h3>Elements</h3><ul>"
    for k,v in config['elements'].items():
        if not k.startswith("__"):
            s += f"""<li><h4>{k}</h4><ul>"""
            if "mixInputs" in v:
                s += f"""<li>LO: {v["mixInputs"]["lo_frequency"]/1e6:.2f} MHz</li> 
                <li>IF: {v["intermediate_frequency"]/1e6:.2f} MHz</li>"""
            s+=f"""<li>Operations: {v.get("operations","None")}</li></ul></li>"""
    s += "</ul>"    
    s += "<h3>Pulses</h3><ul>"
    for k,v in config['pulses'].items():
        if not k.startswith("__"):
            s += f"<li>{k} : {v["operation"]} {v["length"]}ns  {v["waveforms"]}</li>"
    s += "</ul>"
    s += "<h3>Waveforms</h3><ul>"
    for k,v in config['waveforms'].items():
        if not k.startswith("__"):
            s += f"""<li>{k} : {v["type"]} {v["sample"] if v["type"]=="constant" else ""}</li>"""
    s += "</ul>"
    return HTML(s)
    
class QMloader:
    def __init__(self):
        self.button_reload = widgets.Button(description="Reload QM")
        self.button_calibrate = widgets.Button(description="Calibrate QM")
        self.dropdown_config = widgets.Dropdown(options=['config_00','config_qubit',],value='config_00',description='Config:')
        self.output = widgets.Output()
        self.button_reload.on_click(self.reload_qm)
        self.button_calibrate.on_click(self.calibrate_qm)
        self.show()

    def show(self):
        display(widgets.HBox([self.dropdown_config, self.button_reload, self.button_calibrate]), self.output)
    
    def reload_qm(self,button):
        config = importlib.import_module(self.dropdown_config.value)
        importlib.reload(config)
        qm = qmm.open_qm(config.config)
        self.output.append_stdout(f'{time.asctime()} QM is ready with id {qm.id}\n')

    def calibrate_qm(self,button):
        config = importlib.import_module(self.dropdown_config.value)
        importlib.reload(config)
        qm = qmm.open_qm(config.config)
        for k,v in config.calibration_tasks.items():
            for LO,IF_list in v.items():
                for IF in IF_list:
                    self.output.append_stdout(f'Calibrating {k} at {LO/1e6:.1f} + {IF/1e6:.1f} MHz\n')
            qm.calibrate_element(k,v)
            self.output.append_stdout('Done\n')
        qm = qmm.open_qm(config.config)
        self.output.append_stdout(f'{time.asctime()} QM is ready with id {qm.id}\n')


class QueueMonitorSimple(threading.Thread):
    def __init__(self):
        super().__init__()
        self.output = widgets.Output()
        self.QM_label = widgets.Label(value="")
        self.job_table = widgets.HTML(value="")
        self.keeprunning = True
        self.button_stop = widgets.Button(description='Stop')
        self.button_kill = widgets.Button(description='Kill')
        self.progress_bar = widgets.IntProgress(value=0, min=0, max=60)
        self.button_stop.on_click(self.stop)
        self.button_kill.on_click(self.kill)
        self.show()
        self.start()

    def show(self):
        display(widgets.HBox([self.button_stop, self.button_kill]), self.progress_bar, self.QM_label, self.job_table, self.output)

    def run(self):
        while self.keeprunning:
            self.parse_queue()
            time.sleep(0.2)
            self.progress_bar.value = int(time.time()) % 60
        self.output.append_stdout("Done\n")
   
    def stop(self,button):
        self.keeprunning = False
    
    def kill(self,button):
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
            self.job_table.value = ""
            self.QM_label.value = "Error while connecting to the QM"

__killtime__ = {"inf":1e10, "30s":30, "1min":60, "2min":120, "5min":300, }

class QueueMonitor(threading.Thread):
    def __init__(self):
        super().__init__()
        self.button_stop = widgets.Button(description='Stop')
        self.button_kill = widgets.Button(description='Kill')
        self.dropdown_kill = widgets.Dropdown(options=['inf','30s','1min','2min','5min'],value='inf',description='Max time:')
        self.progress_bar = widgets.IntProgress(value=0, min=0, max=60)
        self.QM_label = widgets.Label(value="")
        self.job_table = widgets.HTML(value="")
        self.output = widgets.Output()
        self.keeprunning = True
        self.joblist = []
        self.jobmax = 100
        self.socket1 = context.socket(zmq.SUB)
        self.socket1.bind(f"tcp://{host}:{port1}")
        self.socket1.subscribe("JOB")
        self.button_stop.on_click(self.stop)
        self.button_kill.on_click(self.kill)
        self.show()
        self.start()

    def show(self):
        display(widgets.HBox([self.button_stop, self.button_kill, self.dropdown_kill]), self.progress_bar, self.QM_label, self.job_table, self.output)
        
    def run(self):
        poller = zmq.Poller()
        poller.register(self.socket1, zmq.POLLIN)
        while self.keeprunning:
            evts = dict(poller.poll(timeout=200))
            if self.socket1 in evts:
                topic = self.socket1.recv_string()
                job = self.socket1.recv_json()
                self.joblist.append(job)
                if len(self.joblist)>self.jobmax:
                    self.joblist.pop(0)
            self.parse_queue()
            self.progress_bar.value = int(time.time()) % 60
        self.socket1.close()
        self.output.append_stdout("Done\n")
        
    def stop(self,button):
        self.keeprunning = False
        
    def kill(self,button):
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
        return {"status":status, "time": None, "user": None, "id":job_id, "qm_id":qm_id}
        
    @property
    def killtime(self):
        return __killtime__[self.dropdown_kill.value]
        
    def parse_queue(self):
        try:
            qm_list = qmm.list_open_qms()
            qm = qmm.get_qm(qm_list[0])
            self.QM_label.value = f"Jobs on {qm.id}"
            table = [ self.search_job(job.id,qm.id,"pending") for job in qm.queue.pending_jobs ]
            running_job = qm.get_running_job()
            if running_job:
                job_entry = self.search_job(running_job.id,qm.id,"running")
                table.append(job_entry)
                if job_entry["time"] and (time.time()-job_entry['time'])>self.killtime:
                    running_job.halt()
            self.display_table(table)
            if socket2:
                socket2.send_string("JOBTABLE", flags=zmq.SNDMORE)
                socket2.send_json(table)
        except:
            self.job_table.value = ""
            self.QM_label.value = "Error while connecting to the QM"
    
    def display_table(self, table):
        out = "<table>"
        for job in table:
            waiting_time = f"{time.time()-job["time"]:.0f}" if job["time"] else "--"
            out += f"""<tr><td>{job["status"].capitalize()}</td><td>{job["id"]}</td><td>{job["user"] or "unknown"}</td><td>{waiting_time}s</td></tr>"""
        out += "</table>"
        self.job_table.value = out

def createQueueMonitor(*args):
    try:
        return QueueMonitor(*args)
    except:
        print("Someone already is already listening, falling back to simple QueueMonitor")
        return QueueMonitorSimple(*args)

class Job(threading.Thread):
    def __init__(self, qmprog, blocking=False):
        """Create a QM job from a QUA program with interactive monitoring"""
        self.output = widgets.Output()
        qm_list =  qmm.list_open_qms()
        qm = qmm.get_qm(qm_list[0])
        self.output.append_stdout(f"Sending job to {qm.id}...")
        self.job = qm.queue.add(qmprog)
        while self.job.status=="loading":
            time.sleep(0.1)
        super().__init__()
        self.output.append_stdout("loaded\n")
        self.qm = qm
        self.button_abort = widgets.Button(description='Abort')
        self.button_abort.on_click(self.abort_clicked)
        self.job_table = widgets.HTML(value = "")
        self.show()
        self.abort = False
        self.start()
        if blocking:
            self.join()

    def get_results(self,*args):
        handles = [self.job.result_handles.get(arg) for arg in args]
        return tuple(h.fetch_all(flat_struct=True) for h in handles)
            
    def show(self):
        display(self.button_abort, self.output, self.job_table)

    def abort_clicked(self, button):
        self.abort = True

    def __getattr__(self, attr):
        return getattr(self.job,attr)

    def display(self, table):
        out = "<em>QM job list:</em><table>"
        for job in table:
            waiting_time = f"{time.time()-job["time"]:.0f}" if job["time"] else "--"
            if job["id"]==self.job.id:
                out += f"""<tr><td><b>{job["status"].capitalize()}</b></td><td><b>{job["id"]}</b></td><td><b>{job["user"] or os.environ["JUPYTERHUB_USER"]}</td><td><b>{waiting_time}s</b></td></tr>"""
            else:
                out += f"""<tr><td>{job["status"].capitalize()}</td><td>{job["id"]}</td><td>{job["user"] or "unknown"}</td><td>{waiting_time}s</td></tr>"""
        out += "</table>"
        self.job_table.value = out

    def run(self):
        poller = zmq.Poller()
        socket_info = context.socket(zmq.SUB)
        socket_info.connect(f"tcp://{host}:{port2}")
        socket_info.subscribe("JOBTABLE")
        poller.register(socket_info, zmq.POLLIN)
        if self.job.status=="pending":
            status = {"status":"pending", "time": time.time(), "user":os.environ["JUPYTERHUB_USER"], "id":self.job.id, "qm_id":self.qm.id}
            socket.send_string("JOB", flags=zmq.SNDMORE)
            socket.send_json(status)
        while self.job.status=="pending":
            self.output.append_stdout(f"Position in queue {self.job.position_in_queue()} \r")
            evts = dict(poller.poll(timeout=200))
            if socket_info in evts:
                topic = socket_info.recv_string()
                jobtable = socket_info.recv_json()
                self.display(jobtable)
            if self.abort:
                self.job.cancel()
                self.output.append_stdout("Job has been canceled\n")
                self.job_table.value = ""
                return
        try:
            self.job = self.job.wait_for_execution(timeout=2)
        except:
            self.output.append_stdout("Job has been canceled\n")
            self.job_table.value = ""
            return            
        if self.job.status=="running":
            self.output.append_stdout("Job is running...               \n")
            status = {"status":"running", "time": time.time(), "user":os.environ["JUPYTERHUB_USER"], "id":self.job.id, "qm_id":self.qm.id}
            socket.send_string("JOB", flags=zmq.SNDMORE)
            socket.send_json(status)
        while self.job.status=="running":
            evts = dict(poller.poll(timeout=200))
            if socket_info in evts:
                topic = socket_info.recv_string()
                jobtable = socket_info.recv_json()
                self.display(jobtable)
            if self.abort:
                self.job.halt()
                self.output.append_stdout("Job has been halted\n")
                self.job_table.value = ""
                return
        self.output.append_stdout("Job has finished\n")
        self.job_table.value = ""

    def wait(self):
        result_handles = self.job.result_handles
        while result_handles.is_processing():
            time.sleep(0.5)

class JobSimple:
    def __init__(self, qmprog):
        """Create a QM job from a QUA program"""
        qm_list =  qmm.list_open_qms()
        qm = qmm.get_qm(qm_list[0])
        print(f"Sending job to {qm.id}")
        # Send the QUA program to the OPX, which compiles and executes it
        self.job = qm.queue.add(qmprog)
        # Wait for job to be loaded
        while self.job.status=="loading":
            print("Job is loading...")
            time.sleep(0.1)
        # Wait until job is running
        time.sleep(0.1)
        status = {"status":"pending", "time": time.time(), "user":os.environ["JUPYTERHUB_USER"], "id":self.job.id, "qm_id":qm.id}
        socket.send_string("JOB", flags=zmq.SNDMORE)
        socket.send_json(status)
        while self.job.status=="pending":
            q = self.job.position_in_queue()
            if q>0:
                print(job.id,"Position in queue",q,end='\r')
            time.sleep(0.1)
        self.job=self.job.wait_for_execution()
        print(f"\nJob {self.job.id} is running")
        status = {"status":"running", "time": time.time(), "user":os.environ["JUPYTERHUB_USER"], "id":self.job.id, "qm_id":qm.id}
        socket.send_string("JOB", flags=zmq.SNDMORE)
        socket.send_json(status)
    
    def get_results(self,*args):
        handles = [self.job.result_handles.get(arg) for arg in args]
        return tuple(h.fetch_all(flat_struct=True) for h in handles)

    def __getattr__(self, attr):
        return getattr(self.job,attr) 

    def wait(self):
        result_handles = self.job.result_handles
        while result_handles.is_processing():
            time.sleep(0.5)
        

